"""Acquire a basic snapshot from one verified, rendered Coin map."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import re
import time

from PIL import Image
from playwright.sync_api import Error as BrowserError

import modern_map_reader as ui
from model1_execution import StageFailure
import modern_map_axis_motion as motion
from modern_map_axis import axis_marks
from modern_map_axis_ocr import axis_scale_live
from modern_map_cursor import read_cursor_pair
from modern_map_cells import _agree, merge_cells
from modern_map_basic_numbers import (HORIZON_SECONDS, REALIZED_24H, basic_target, closing_realized_update,
                                      fact_rows, market_amounts, period_seconds, verified_number, verify_maximum)
from modern_map_evidence import box_member, equivalent, iso, number, sample_groups, source_clock, verified_member
from modern_map_overview import _FIELDS, after_read_failure, overview
from modern_map_surface import prepare_surface
from modern_map_tooltip import read_tooltip


def require(test, reason):
    if not test:
        error=StageFailure('source_not_readable','hover_evidence_mismatch')
        error.basic_reason=reason
        raise error


def identity(page, symbol, model, timeframe):
    ui.gate(page)
    require(page.url==ui.map_url(symbol,model) and ui.choice(page,f'Model {model}') and ui.choice(page,'Coin'),
            'map_identity_changed')
    ui.symbol_control(page,symbol)
    selection=ui.selected_range(page,timeframe)
    expected={'Layout':'Single chart','Lens':'Heat'}
    require(all(ui.option_control(page,key).inner_text().strip()==value for key,value in expected.items()),
            'map_selection_changed')
    require(re.fullmatch(r'Viridis(?:\s*Default)?',ui.option_control(page,'Colours').inner_text().strip())
            and ui.full_intensity(page),'colour_or_intensity_changed')
    return selection


def changed_observation(condition, **witness):
    error = StageFailure('source_not_readable', 'hover_evidence_mismatch')
    error.basic_reason = condition
    error.verified_change = dict(condition=condition, **witness)
    raise error


def _without_missing_maximum(before, after):
    """Only publish the optional maximum/price if both observations report it."""
    fields = ('raw_largest_value', 'raw_largest_price')
    omit = {k for k in fields if before.get(k) != after.get(k) and
            (before.get(k) is None or after.get(k) is None)}
    if not omit:
        return before, after
    if 'raw_largest_value' in omit:
        omit.add('raw_largest_price')
    copies = []
    for facts in (before, after):
        # Losing an optional field cannot make an originally contradictory maximum valid.
        if facts.get('raw_largest_price') is not None:
            verified_number(facts['raw_largest_price'], positive=True)
        verify_maximum(market_amounts(facts)['raw_largest_value'], facts.get('raw_largest_price'), fact_rows(facts))
        values = {**facts, **dict.fromkeys(omit)}
        values['display_values'] = {k:v for k,v in facts.get('display_values', {}).items() if k not in omit}
        copies.append(values)
    return tuple(copies)


def prove_observations(before, after, emit=None, *, base_only=False):
    # A refresh requires a positive, still valid source observation. Missing or
    # contradictory source fields are failures, not evidence that the market moved.
    if base_only:
        before, after = _without_missing_maximum(before, after)
    for key in (*_FIELDS,'source_time','sidebar_levels','raw_sidebar_rows','unreadable_sidebar_containers',
                'ambiguous_fields','duplicate_sidebar_levels'):
        if before.get(key)!=after.get(key):
            if base_only and (key in REALIZED_24H or key == 'raw_last_price'):
                continue
            if emit:
                emit('stage',dict(name='basic_compare_market',status='failed',
                    expected={key:before.get(key)},received={key:after.get(key)}))
            if (base_only and key not in ('unreadable_sidebar_containers', 'ambiguous_fields')
                    and before.get(key) is not None and after.get(key) is not None
                    and not after.get('ambiguous_fields') and not after.get('unreadable_sidebar_containers')):
                ui.core_observation(after)
                market_amounts(after)
                fact_rows(after)
                if key in ('raw_updated_at', 'source_time'):
                    # Clocks which moved backwards are not newer source observations.
                    if not before.get('source_time') or after['source_time'] <= before['source_time']:
                        require(False,'source_epoch_or_values_changed')
                changed_observation('source_epoch_or_values_changed', field=key,
                                    opening_source_time=before['source_time'], closing_source_time=after['source_time'])
            require(False,'source_epoch_or_values_changed')
    ui.core_observation(after)
    if base_only:
        closing_realized_update(before, after)
    return before, after


def right_edge_regions(image, bounds, side, scale, model):
    raw_ref, where=scale
    reference=float(number(raw_ref)[0])
    rail=side.get('sidebar_levels') or []
    require(rail,'native_grid_anchor_missing')
    require(not {'sidebar_levels','sidebar_shape'}.intersection(side.get('ambiguous_fields',[]))
            and not side.get('unreadable_sidebar_containers'),'sidebar_shape_incomplete')
    anchor=float(number(rail[0]['raw_price'])[0])
    step=float(number(side['raw_price_step'])[0])
    counts={}
    height,runs=sample_groups(image,bounds,model,anchor,step,where,reference,audit=counts)
    regions=[{'side':rows[0]['side'],'colour':colour,'prices':[r['price'] for r in rows]}
             for colour,rows in runs]
    return regions,counts['native_bins_inspected'],height


def edge_open(surface,bounds,box,*,include_time=False):
    # A historical-only card does not hide the current price column.
    _,top,right,bottom=bounds
    axis_end=box[0]+box[2]
    for x1,y1,x2,y2 in surface['unknown']+surface['covered']:
        if x1<axis_end and x2>right-3 and y1<bottom+1 and y2>top:
            return False
        if include_time and x1<axis_end and x2>max(box[0],right-115) and y1<box[1]+box[3] and y2>bottom:
            return False
    return True


def source_screen(page, box, padded_size, guard):
    x,y,w,h=box
    data=ui.protected_clip(page,{'x':x,'y':y,'width':w,'height':h},guard)
    with Image.open(BytesIO(data)) as crop:
        require(crop.size==(w,h),'closing_chart_clip_size')
        result=Image.new('RGB',padded_size)
        result.paste(crop,(x,y))
        return result


def read_epoch(stamp, update, period=None):
    return source_clock(stamp,datetime.fromisoformat(update),column=True,max_lag=period_seconds(period) if period else 1200)



def displayed_column(page,box,bounds,column,same_document,emit,*,base_only=False,period=None,opening=False,
                     pointer_position=None,timeframe=None,source_update=None):
    if base_only:
        same_document()
        phase='opening' if opening else 'closing'
        def probe(role,status,receipt):
            emit('stage',dict(name=phase+'_'+role+'_cursor_probe',status=status,**receipt))
        def refreshed_anchor():
            same_document()
            source = overview(page)
            require(source.get('source_time') and 'source_time' not in source.get('ambiguous_fields', []),
                    'source_clock_unconfirmed')
            return datetime.fromisoformat(source['source_time'])
        prior,current,sample=read_cursor_pair(page,box,bounds,source_update or column,timeframe,period,
                                               expected_column=None if opening else column,observe=probe,
                                               refresh_anchor=None if opening else refreshed_anchor)
        same_document()
        if pointer_position is not None:
            pointer_position.update(y=sample['pointer_y'],prior_marks=prior['marks'],sample=sample)
        emit('stage',dict(name=('opening' if opening else 'closing')+'_cursor_columns',status='ok',**sample))
        return current['stamp'],current['marks']

    # Enriched readers preserve their existing independent last-column check.
    _,top,right,bottom=bounds
    x,y=right-1,(top+bottom)//2
    try:
        return ui.read_paint(page,box,x,y,observe=emit)
    except StageFailure as error:
        if error.code!='source_not_readable' or error.reason is not None:
            raise
    same_document()
    try:
        event=read_tooltip(page,box,bounds,column,x,y,ui.PAINT,
                           deadline=time.monotonic()+ui.limited_timeout(1000)/1000)
        emit('hover',event)
        stamps={m[0] for m in event.get('marks',()) if re.fullmatch(r'\d{1,2}/\d{1,2} \d{1,2}:\d{2}',m[0])}
        require(event.get('price_label_source') in ('dom','canvas') and bool(event.get('raw_price_labels')) and
                len(stamps)==1,'closing_column_display_unavailable')
        stamp=stamps.pop()
        require(read_epoch(stamp,iso(column))==column,'closing_column_changed')
        same_document()
        emit('stage',dict(name='closing_column_from_visible_tooltip',status='ok',source_timestamp=stamp))
        return stamp,[]
    finally:
        page.mouse.move(max(0,box[0]-2),max(0,box[1]-2))


def enrich_latest(page,box,bounds,side,column,scale,regions,height,deadline,stage,emit):
    """Try one tooltip and one missing bin; use a known-value control only for a leaderless box."""
    raw_reference,where=scale
    raw_step=side['raw_price_step']
    step=float(number(raw_step)[0])
    rail=side['sidebar_levels']
    anchor=rail[0]['raw_price']
    known={number(r['raw_price'])[0]: r for r in rail}
    prices=[p for region in regions for p in region['prices']]
    def uncovered():
        return [p for p in prices if not any(abs(float(q)-p)<=step*.35 for q in known)]
    missing=uncovered()
    if not missing:return []
    def remaining():
        return max(0,min(deadline-time.monotonic(),ui.limited_timeout(5000)/1000))
    def attempt(name,fn):
        try:
            return stage(name,fn)
        except StageFailure as exc:
            if exc.code not in ('source_not_readable','source_timeout'):raise
        except BrowserError:
            pass
        return None
    def skipped(reason):
        emit('stage',dict(name='basic_optional_measure',status='skipped',condition=reason))
    def entry(member,method):
        return dict(member,source=method,source_time=side['source_time'],column_time=iso(column),raw_price_step=raw_step)

    left,top,right,bottom=bounds
    book={}
    printed=[]
    def hover():
        event=read_tooltip(page,box,bounds,column,right-1,where(missing[0]),ui.PAINT,
                           deadline=time.monotonic()+min(1.9,remaining()))
        emit('hover',event)
        merge_cells(book,event['rows'],column.strftime('%m/%d %H:%M'),raw_step)
        printed.extend(event.get('raw_price_labels',[]))
    if remaining()>0:
        attempt('basic_optional_hover',hover)
    known.update(book)
    enriched=[entry(m,'hover') for m in book.values()]
    missing=uncovered()
    if not missing:
        skipped('all_significant_bins_already_priced')
        return enriched
    if remaining()<1.6:
        skipped('insufficient_time_for_native_selection')
        return enriched
    def printed_price(p):
        candidates=[v for v in printed if abs(float(number(v)[0])-p)<=step*.35]
        return candidates[0] if len(candidates)==1 else None
    # A verified tooltip axis label can still bind a Box selection when the
    # native Largest-cluster row has no readable price. Prefer that opportunity.
    target=next((p for p in missing if printed_price(p)),missing[0])
    def measure(p,raw_price):
        y=where(p)
        start,end=max(top,y-height*.39),min(bottom,y+height*.39)
        require(top<=start<end<=bottom,'single_bin_outside_price_domain')
        result=ui.measure_box(page,bounds,start,end,require_leader=not bool(raw_price))
        require(number(result[0])[0]==number(raw_reference)[0],'single_bin_reference_changed')
        leader=result[2]
        if leader is not None:
            if raw_price:
                require(number(leader['raw_price'])[0]==number(raw_price)[0],'single_bin_price_conflict')
            equivalent(result[1],[leader['raw_value']])
            raw_price=leader['raw_price']
        # Reject multi-bin selections, adjacent stale panels and unprinted
        # price centres regardless of where the cursor was placed.
        member=box_member(result,raw_price,raw_step,anchor)
        verified_member({'price':p},member,step)
        return member,leader is not None
    def read_one():
        evidence=measure(target,printed_price(target))
        member,has_leader=evidence
        if not has_leader:
            require(remaining()>=1.6,'insufficient_time_for_single_bin_control')
            maximum=side.get('raw_largest_value')
            controls=[r for r in rail if maximum and number(r['raw_value'],True)[0]==number(maximum,True)[0]
                      and top+height*.39<=where(float(number(r['raw_price'])[0]))<=bottom-height*.39]
            require(controls,'missing_independent_single_bin_control')
            control=controls[0]
            def witness():
                observed,_=measure(float(number(control['raw_price'])[0]),control['raw_price'])
                equivalent(control['raw_value'],[observed['raw_value']])
            stage('basic_known_amount_control',witness)
        return entry(member,'measure')
    try:
        with ui.map_access_budget(remaining()):
            item=attempt('basic_optional_measure',read_one)
        if item is not None:enriched.append(item)
    finally:
        ui.select_choice(page,'Pan')
    return enriched


@ui.phase('map_read')
def capture_basic(page,symbol,model,timeframe,output,*,guard=None,observe=None,base_only=False):
    motion_budget = set()
    def capture_once(restarted):
        return _capture_basic_observation(page, symbol, model, timeframe, output, guard=guard, observe=observe,
                                          base_only=base_only, restarted=restarted, motion_budget=motion_budget)
    try:
        return capture_once(None)
    except StageFailure as error:
        change = getattr(error, 'verified_change', None)
        if (not base_only or not change or error.code != 'source_not_readable' or
                getattr(error,'secondary_guard_code',None)):
            raise
        # Retain the first cause without retaining a Python traceback with full chart images.
        witness = dict(change)
    ui.limited_timeout(1)
    if observe:
        observe('observation_restart', witness)
    if isinstance(output, BytesIO):
        output.seek(0)
        output.truncate()
    else:
        Path(output).unlink(missing_ok=True)
    return capture_once(witness)


def _capture_basic_observation(page, symbol, model, timeframe, output, *, guard, observe, base_only,
                               restarted, motion_budget):
    from modern_map_basic_numbers import build_basic_snapshot
    require(basic_target(symbol,model,timeframe),'wrong_basic_target')
    def emit(kind,value):
        if observe:return observe(kind,value)
    def stage(name,fn):
        at=time.monotonic()
        try:
            value=fn()
        except Exception as error:
            emit('stage',dict(name=name,status='failed',condition=getattr(error,'basic_reason',None)
                              or getattr(error,'reason',None) or type(error).__name__,
                              elapsed_ms=round(1000*(time.monotonic()-at))))
            raise
        emit('stage',dict(name=name,status='ok',elapsed_ms=round(1000*(time.monotonic()-at))))
        return value

    def protected():
        if guard:guard()
        ui.clear_cookie_notice(page)
    if guard:guard()
    ui.clear_cookie_notice(page,model=model)
    if restarted:
        stage('reobserve_settled_chart',lambda:ui.open_verified_map(
            page,symbol,model,timeframe,guard=guard,observe=observe,verify_selected_only=True))
    ui.display_ready(page)
    doc=page.evaluate_handle('()=>document')
    def same_document():
        require(not page.is_closed() and doc.evaluate('d=>d===document'),'source_document_changed')
        protected()
    try:
        start_range=stage('basic_reobserve_identity' if restarted else 'basic_identity_start',lambda:identity(page,symbol,model,timeframe))
        side=stage('read_market_overview',lambda:overview(page))
        emit('overview',side)
        def validate_market():
            if base_only:
                market_amounts(side)
            return ui.core_observation(side)
        stage('verify_market_observation',validate_market)
        with ui.position_chart(page,protected) as initial_box:
            surface=stage('inspect_chart_obstruction',lambda:prepare_surface(page,initial_box,protected,ui.limited_timeout))
            box=ui.chart_box(page)
            controls=ui.evidence_controls(page,symbol,model)
            page.mouse.move(max(0,box[0]-2),max(0,box[1]-2))
            with stage('capture_chart_and_controls',lambda:ui.market_surface(page,box,controls,protected)) as screen:
                for area in surface['unknown']:screen.paste((10,13,18),tuple(area))
                stage('save_filtered_chart',lambda:ui.save_evidence(screen,box,controls,output))
                png=output.getvalue() if isinstance(output,BytesIO) else Path(output).read_bytes()
                emit('png',png)
                digest=sha256(png).hexdigest()
                bounds=stage('locate_dark_price_plot',lambda:ui.dark_bounds(screen,box))
                require(edge_open(surface,bounds,box,include_time=base_only),'latest_price_edge_obstructed')
                left,top,right,bottom=bounds
                middle=(top+bottom)//2
                opening_pointer = {}
                if base_only:
                    current,initial_marks=stage('read_latest_crosshair_time',lambda:displayed_column(
                        page,box,bounds,datetime.fromisoformat(side['source_time']),same_document,emit,
                        base_only=True,period=side['raw_column_period'],opening=True,timeframe=timeframe,
                        pointer_position=opening_pointer))
                    prior_marks=opening_pointer['prior_marks']
                else:
                    prior,prior_marks=stage('read_prior_crosshair_time',lambda:ui.read_paint(
                        page,box,left+.72*(right-left),middle,observe=emit))
                    current,initial_marks=stage('read_latest_crosshair_time',lambda:ui.read_paint(page,box,right-1,middle,observe=emit))
                column=read_epoch(current,side['source_time'],side.get('raw_column_period'))
                if restarted:
                    floors = ((column, restarted.get('closing_column_time') or restarted.get('column_time')),
                              (datetime.fromisoformat(side['source_time']), restarted.get('closing_source_time')))
                    require(all(floor is None or observed >= datetime.fromisoformat(floor) for observed,floor in floors),
                            'new_observation_predates_confirmed_change')
                if not base_only:
                    previous=source_clock(prior,column,column=True,max_lag=HORIZON_SECONDS[timeframe])
                    require(0<(column-previous).total_seconds()<=HORIZON_SECONDS[timeframe] and
                            column<=datetime.fromisoformat(side['source_time']),'latest_column_not_established')
                marks=stage('read_numeric_price_axis',lambda:axis_marks(page,box,prior_marks,initial_marks))
                ocr_cache={}
                def read_axis(phase,frame,chart,plot,labels,reference,pointer):
                    receipt={}
                    result=None
                    try:
                        result=axis_scale_live(frame,chart,plot,labels,reference=reference,observe=receipt.update,
                                              timeout=ui.limited_timeout(10000)/1000,cache=ocr_cache,
                                              pointer_y=pointer.get('y'))
                        if result is None and base_only and pointer.get('sample') and phase not in motion_budget:
                            mark=motion.ambiguous_mark(receipt)
                            if mark is not None:
                                motion_budget.add(phase)
                                same_document()
                                identity(page,symbol,model,timeframe)
                                result=motion.resolve(page,frame,chart,plot,labels,reference,pointer['sample'],
                                                      ocr_cache,receipt,mark,phase,same_document,emit)
                                same_document()
                                identity(page,symbol,model,timeframe)
                    finally:
                        emit('stage',dict(name='basic_'+phase+'_price_readout',status='observed' if result else 'failed',
                                          **receipt))
                    require(result is not None,receipt.get('condition','axis_unverified'))
                    return result,receipt.get('method','dom_canvas')
                scale,source_method=stage('basic_verified_price_axis',lambda:read_axis(
                    'opening',screen,box,bounds,marks,side.get('raw_last_price'),opening_pointer))
                raw_ref,where=scale
                regions,native_bins,bin_height=stage('basic_classify_latest_edge',lambda:right_edge_regions(
                    screen,bounds,side,scale,model))
                start=dict(source_time=side['source_time'],column_time=iso(column),raw_last_price=raw_ref,
                           raw_total=side['raw_total'],raw_above_total=side['raw_above_total'],
                           raw_below_total=side['raw_below_total'],sidebar_rows=len(side['sidebar_levels']),
                           chart_regions=len(regions),native_bins_inspected=native_bins)
                emit('stage',dict(name='basic_start_observation',status='ok',reference_method=source_method,chart_groups=regions,**start))

                def closing_arguments(prefix=''):
                    def record(name,fn):
                        return stage(prefix+name,fn)
                    same_document()
                    with (ui.position_chart(page,protected) if base_only else nullcontext(box)) as closing_box:
                        require(ui.chart_box(page)==closing_box,'chart_geometry_changed')
                        if base_only:
                            if not ui.choice(page,'Pan'):
                                ui.select_choice(page,'Pan')
                            require(ui.choice(page,'Pan'),'closing_pointer_mode_unverified')
                            page.mouse.move(max(0,closing_box[0]-2),max(0,closing_box[1]-2))
                            viewport=page.viewport_size or {}
                            cx,cy,cw,ch=closing_box
                            require(0<=cx<cx+cw<=viewport.get('width',0) and 0<=cy<cy+ch<=viewport.get('height',0),
                                    'closing_chart_outside_viewport')
                            current_surface=prepare_surface(page,closing_box,protected,ui.limited_timeout)
                            require(ui.chart_box(page)==closing_box,'chart_geometry_changed')
                        with record('basic_closing_chart',lambda:source_screen(page,closing_box,(viewport['width'],viewport['height']) if base_only else screen.size,protected)) as final:
                            closing_bounds=ui.dark_bounds(final,closing_box)
                            if base_only:
                                clear=edge_open(current_surface,closing_bounds,closing_box,include_time=True)
                                emit('stage',dict(name='basic_closing_geometry',status='ok' if clear else 'blocked',
                                    canvas_rect=list(closing_box),plot_rect=list(closing_bounds),
                                    opening_canvas_rect=list(box),opening_plot_rect=list(bounds),
                                    viewport=[viewport['width'],viewport['height']],geometry_changed=box!=closing_box or bounds!=closing_bounds,
                                    covered=len(current_surface['covered']),unknown=len(current_surface['unknown'])))
                                require(clear,'closing_price_or_time_axis_obstructed')
                            else:
                                require(closing_bounds==bounds,'closing_price_domain_changed')
                            closing_pointer = {}
                            close_time,close_marks=record('recheck_latest_column',lambda:displayed_column(
                                page,closing_box,closing_bounds,column,same_document,emit,base_only=base_only,period=side.get('raw_column_period'),
                                pointer_position=closing_pointer,timeframe=timeframe,source_update=datetime.fromisoformat(side['source_time'])))
                            require(read_epoch(close_time,side['source_time'],side.get('raw_column_period'))==column,'closing_column_changed')
                            end_marks=axis_marks(page,closing_box,closing_pointer.get('prior_marks',prior_marks if box==closing_box else []),close_marks)
                            # Collect a fresh overview after the closing image and cursor
                            # observation. Its explicitly labeled price can supply the tag.
                            after=record('recheck_market_update',lambda:overview(page))
                            verified_side,after=record('basic_verify_closing_market',lambda:prove_observations(side,after,emit,base_only=base_only))
                            end_scale,_=record('basic_closing_price_axis',lambda:read_axis(
                                'closing',final,closing_box,closing_bounds,end_marks,after.get('raw_last_price'),closing_pointer))
                            if number(end_scale[0])[0]!=number(raw_ref)[0]:
                                emit('stage',dict(name='basic_compare_reference',status='failed',
                                    expected={'reference':raw_ref},received={'reference':end_scale[0]}))
                                if base_only:
                                    changed_observation('closing_reference_changed', column_time=iso(column))
                                require(False,'closing_reference_changed')
                            # Only the last plotted column determines significant regions.
                            # Equal samples on the same translated scale avoid a second
                            # classification; repainting is accepted if semantic groups agree.
                            l,t,r,b=closing_bounds
                            step=float(number(side['raw_price_step'])[0])
                            ref=float(number(raw_ref)[0])
                            dy=t-top
                            same_grid=(b-t==bottom-top and where(ref)+dy==end_scale[1](ref)
                                       and where(ref+step)+dy==end_scale[1](ref+step))
                            def edge_bytes(image,x,y1,y2):
                                with image.crop((x-1,y1,x,y2+1)) as stripe:
                                    return stripe.tobytes()
                            if same_grid and edge_bytes(screen,right,top,bottom)==edge_bytes(final,r,t,b):
                                emit('stage',dict(name='basic_closing_regions',status='reused'))
                            else:
                                final_regions,final_bins,_=right_edge_regions(final,closing_bounds,side,end_scale,model)
                                if final_regions!=regions or final_bins!=native_bins:
                                    emit('stage',dict(name='basic_compare_regions',status='failed',
                                        expected=dict(chart_regions=regions,native_bins_inspected=native_bins),
                                        received=dict(chart_regions=final_regions,native_bins_inspected=final_bins)))
                                    if base_only:
                                        changed_observation('closing_significant_regions_changed', column_time=iso(column))
                                    require(False,'closing_significant_regions_changed')
                            require(ui.chart_box(page)==closing_box,'closing_geometry_changed_at_pointer')
                    end_range=record('basic_identity_end',lambda:identity(page,symbol,model,timeframe))
                    require(start_range==end_range, 'closing_range_changed')
                    same_document()
                    end=dict(start,source_time=after['source_time'],column_time=iso(read_epoch(close_time,after['source_time'],after.get('raw_column_period'))),
                             raw_last_price=end_scale[0],same_document=True,settings_verified=True,same_market_labels=True,
                             symbol=symbol,heatmap_model=model,timeframe=timeframe,mode='Coin',range_control=end_range)
                    emit('stage',dict(name=prefix+'basic_end_observation',status='ok',**end))
                    return verified_side, dict(symbol=symbol,model=model,timeframe=timeframe,source_url=page.url,
                              source_time_after=after['source_time'],column_time=iso(column),column_time_after=end['column_time'],
                              cursor_proof=(dict(schema='coinglass.cursor.v1',opening=opening_pointer['sample'],
                                                 closing=closing_pointer['sample']) if base_only else None),
                              raw_last_price=raw_ref,source_sha256=digest,chart_regions=regions,provenance='live',
                              native_bins_inspected=native_bins,captured_at=iso(datetime.now(timezone.utc)),end_observation={**after,**end})

                verified_side,basic_args=closing_arguments()
                base=stage('build_basic_result',lambda:build_basic_snapshot(verified_side,**basic_args,require_fresh_evidence=base_only))
                if emit('verified_basic',base) is False:
                    raise StageFailure('state_store_failed')
                if base_only:
                    return base
                try:
                    try:
                        enrichment=enrich_latest(page,box,bounds,side,column,scale,regions,bin_height,
                                                 time.monotonic()+5,stage,emit)
                    finally:
                        page.mouse.move(max(0,box[0]-2),max(0,box[1]-2))
                    known={number(row['raw_price'])[0]:number(row['raw_value'],True) for row in base['levels']}
                    novel=False
                    for row in enrichment:
                        prior=known.get(number(row['raw_price'])[0])
                        if prior is None:
                            novel=True
                        else:
                            require(_agree(prior,number(row['raw_value'],True)),'optional_overlap_amount_conflict')
                    if enrichment and not novel:
                        emit('stage',dict(name='optional_overlap_only',status='completed',count=len(enrichment)))
                    if novel:
                        _,args=closing_arguments('enrichment_')
                        return stage('validate_enriched_result',lambda:build_basic_snapshot(side,enrichment=enrichment,**args))
                except StageFailure as error:
                    if error.code not in ('source_not_readable','source_timeout'):
                        raise
                    emit('stage',dict(name='enrichment_discarded',status='skipped',
                                      condition=getattr(error,'basic_reason',None) or error.reason or error.code))
                except BrowserError:
                    emit('stage',dict(name='enrichment_discarded',status='skipped',condition='browser_unavailable'))
                return base
    except Exception as error:
        if getattr(error, 'verified_change', None) and not restarted:
            # Keep the handoff on the very same authenticated document. A source
            # redirect or a contradictory selection must retain its own failure.
            same_document()
            stage('verify_changed_map_identity',lambda:identity(page,symbol,model,timeframe))
            if 'digest' in locals():
                error.verified_change['discarded_image_sha256'] = digest
        if observe and not (base_only and not restarted and getattr(error, 'verified_change', None)):
            after_read_failure(page,same_document,emit)
        raise
    finally:
        try:doc.dispose()
        except BrowserError:pass
