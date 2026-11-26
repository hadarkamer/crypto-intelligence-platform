"""Acquire one BTC/1/12H basic snapshot from its rendered, protected Coin map."""
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
from modern_map_axis import axis_marks
from modern_map_axis_ocr import axis_scale_live
from modern_map_cells import _agree, merge_cells
from modern_map_basic_numbers import market_amounts, period_seconds
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
    require(page.url==ui.map_url(symbol) and ui.choice(page,f'Model {model}') and ui.choice(page,'Coin'),
            'map_identity_changed')
    ui.symbol_control(page,symbol)
    expected={'Range':ui.RANGES[timeframe],'Layout':'Single chart','Lens':'Heat'}
    require(all(ui.option_control(page,key).inner_text().strip()==value for key,value in expected.items()),
            'map_selection_changed')
    require(re.fullmatch(r'Viridis(?:\s*Default)?',ui.option_control(page,'Colours').inner_text().strip())
            and ui.full_intensity(page),'colour_or_intensity_changed')


def prove_observations(before, after):
    # Compare dates and numerical source fields, including the entire visible
    # rail. Unlike a number-only total, this also detects changed row content.
    for key in (*_FIELDS,'source_time','sidebar_levels','raw_sidebar_rows','unreadable_sidebar_containers',
                'ambiguous_fields','duplicate_sidebar_levels'):
        require(before.get(key)==after.get(key),'source_epoch_or_values_changed')
    ui.core_observation(after)


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


def from_native_box(page, bounds, side):
    _,top,_,bottom=bounds
    # One domain selection can establish a printed reference and coordinate
    # scale. A largest-cluster value is irrelevant to this operation.
    result=ui.measure_box(page,bounds,top+.2,bottom-.2,require_leader=False)
    raw_ref,total,_,low,high=result
    equivalent(total,[side['raw_above_total'],side['raw_below_total']])
    require(low<number(raw_ref)[0]<high,'measured_reference_outside_domain')
    def where(price):
        return bottom-.2-(price-float(low))*(bottom-top-.4)/float(high-low)
    return raw_ref,where


def source_screen(page, box, padded_size, guard):
    x,y,w,h=box
    data=ui.protected_clip(page,{'x':x,'y':y,'width':w,'height':h},guard)
    with Image.open(BytesIO(data)) as crop:
        require(crop.size==(w,h),'closing_chart_clip_size')
        result=Image.new('RGB',padded_size)
        result.paste(crop,(x,y))
        return result


def read_epoch(stamp, update):
    return source_clock(stamp,datetime.fromisoformat(update),column=True)



def displayed_column(page,box,bounds,column,same_document,emit,*,base_only=False,period=None,opening=False):
    """Observe a displayed time. At opening the supplied date is an anchor only."""
    phase='opening' if opening else 'closing'
    def verify(stamp):
        actual=read_epoch(stamp,iso(column))
        if opening:
            require(0<=(column-actual).total_seconds()<=period_seconds(period),'latest_column_not_within_period')
        else:
            require(actual==column,'closing_column_changed')
    _,top,right,bottom=bounds
    x,y=right-1,(top+bottom)//2
    try:
        stamp,marks=ui.read_paint(page,box,x,y,observe=emit,fresh_pointer=base_only,pointer_phase=phase)
        if base_only:
            verify(stamp)
            emit('stage',dict(name=phase+'_column_from_visible_crosshair',status='ok',
                              source_timestamp=stamp,pointer_x=round(x),pointer_y=round(y)))
        return stamp,marks
    except StageFailure as error:
        if error.code!='source_not_readable' or error.reason is not None:
            raise
    same_document()
    if base_only:
        # A different height on the same verified last column can expose the
        # tooltip when its middle intersects the reference line or a price tag.
        y=round(top+(bottom-top)*.35)
        require(ui.chart_box(page)==box,'closing_geometry_changed_at_pointer')
    try:
        event=read_tooltip(page,box,bounds,column,x,y,ui.PAINT,
                           deadline=time.monotonic()+ui.limited_timeout(1000)/1000,clock_only=base_only)
        if base_only:
            event=dict(event,expected='dated_last_column_header_at_pointer',x=round(x),y=round(y))
        emit('hover',event)
        if base_only:
            evidence=event.get('column_evidence',[])
            stamps={item['dated_title'] for item in evidence}
            require(len(stamps)==1,phase+'_column_display_unavailable')
            require(all(item['period_seconds']==period_seconds(period) for item in evidence),phase+'_column_period_changed')
            stamp=stamps.pop()
            verify(stamp)
            same_document()
            emit('stage',dict(name=phase+'_column_from_visible_tooltip',status='ok',
                              source_timestamp=stamp,pointer_x=round(x),pointer_y=round(y)))
            return stamp,[]
        # price_labels() already checked this *newly displayed* table has one
        # full-datetime title, regular times ending at column, and a price axis.
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
    from modern_map_basic_numbers import build_basic_snapshot
    require((symbol,model,timeframe)==('BTC',1,'12H'),'wrong_basic_target')
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
    ui.display_ready(page)
    doc=page.evaluate_handle('()=>document')
    def same_document():
        require(not page.is_closed() and doc.evaluate('d=>d===document'),'source_document_changed')
        protected()
    try:
        stage('basic_identity_start',lambda:identity(page,symbol,model,timeframe))
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
                prior,prior_marks=stage('read_prior_crosshair_time',lambda:ui.read_paint(
                    page,box,left+.72*(right-left),middle,observe=emit))
                def read_opening_column():
                    if base_only:
                        return displayed_column(page,box,bounds,datetime.fromisoformat(side['source_time']),same_document,emit,
                                                base_only=True,period=side.get('raw_column_period'),opening=True)
                    return ui.read_paint(page,box,right-1,middle,observe=emit)
                current,initial_marks=stage('read_latest_crosshair_time',read_opening_column)
                column=read_epoch(current,side['source_time'])
                previous=source_clock(prior,column,column=True,max_lag=12*3600)
                require(0<(column-previous).total_seconds()<=12*3600 and
                        column<=datetime.fromisoformat(side['source_time']),'latest_column_not_established')
                marks=stage('read_numeric_price_axis',lambda:axis_marks(page,box,prior_marks,initial_marks))
                scale=stage('basic_verified_price_axis',lambda:axis_scale_live(screen,box,bounds,marks))
                source_method='chart_axis'
                if scale is None:
                    source_method='measure_domain'
                    with ui.map_access_budget(ui.limited_timeout(4000)/1000):
                        scale=stage('basic_native_price_domain',lambda:from_native_box(page,bounds,side))
                    ui.select_choice(page,'Pan')
                raw_ref,where=scale
                if side['raw_last_price']:
                    require(number(raw_ref)[0]==number(side['raw_last_price'])[0],'reference_price_conflict')
                regions,native_bins,bin_height=stage('basic_classify_latest_edge',lambda:right_edge_regions(
                    screen,bounds,side,scale,model))
                start=dict(source_time=side['source_time'],column_time=iso(column),raw_last_price=raw_ref,
                           raw_total=side['raw_total'],raw_above_total=side['raw_above_total'],
                           raw_below_total=side['raw_below_total'],sidebar_rows=len(side['sidebar_levels']),
                           chart_regions=len(regions),native_bins_inspected=native_bins)
                emit('stage',dict(name='basic_start_observation',status='ok',reference_method=source_method,**start))

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
                            close_time,close_marks=record('recheck_latest_column',lambda:displayed_column(
                                page,closing_box,closing_bounds,column,same_document,emit,base_only=base_only,period=side.get('raw_column_period')))
                            require(read_epoch(close_time,side['source_time'])==column,'closing_column_changed')
                            end_marks=axis_marks(page,closing_box,prior_marks if box==closing_box else [],close_marks)
                            end_scale=record('basic_closing_price_axis',lambda:axis_scale_live(final,closing_box,closing_bounds,end_marks))
                            if end_scale is None:
                                # Reuse the independently proven opening mapping only
                                # if the complete right-axis band is byte-identical.
                                l,t,r,b=closing_bounds
                                dx,dy=l-left,t-top
                                require((right+dx,bottom+dy)==(r,b),'closing_price_domain_changed')
                                with screen.crop((right+1,top,box[0]+box[2],bottom+1)) as a, final.crop(
                                        (r+1,t,closing_box[0]+closing_box[2],b+1)) as end_axis:
                                    require(a.size[0]>=14 and a.size==end_axis.size and a.tobytes()==end_axis.tobytes(),
                                            'closing_axis_unverified')
                                end_scale=(raw_ref,lambda p:where(p)+dy)
                            require(number(end_scale[0])[0]==number(raw_ref)[0],'closing_reference_changed')
                            final_regions,final_bins,_=right_edge_regions(final,closing_bounds,side,end_scale,model)
                            require(final_regions==regions and final_bins==native_bins,'closing_significant_regions_changed')
                            require(ui.chart_box(page)==closing_box,'closing_geometry_changed_at_pointer')
                    after=record('recheck_market_update',lambda:overview(page))
                    record('basic_verify_closing_market',lambda:prove_observations(side,after))
                    record('basic_identity_end',lambda:identity(page,symbol,model,timeframe))
                    same_document()
                    end=dict(start,source_time=after['source_time'],column_time=iso(read_epoch(close_time,after['source_time'])),
                             raw_last_price=end_scale[0],same_document=True,settings_verified=True,same_market_labels=True,
                             symbol=symbol,heatmap_model=model,timeframe=timeframe,mode='Coin')
                    emit('stage',dict(name=prefix+'basic_end_observation',status='ok',**end))
                    return dict(symbol=symbol,model=model,timeframe=timeframe,source_url=ui.map_url(symbol),
                              source_time_after=after['source_time'],column_time=iso(column),column_time_after=end['column_time'],
                              raw_last_price=raw_ref,source_sha256=digest,chart_regions=regions,provenance='live',
                              native_bins_inspected=native_bins,captured_at=iso(datetime.now(timezone.utc)),end_observation={**after,**end})

                basic_args=closing_arguments()
                base=stage('build_basic_result',lambda:build_basic_snapshot(side,**basic_args))
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
                        args=closing_arguments('enrichment_')
                        return stage('validate_enriched_result',lambda:build_basic_snapshot(side,enrichment=enrichment,**args))
                except StageFailure as error:
                    if error.code not in ('source_not_readable','source_timeout'):
                        raise
                    emit('stage',dict(name='enrichment_discarded',status='skipped',
                                      condition=getattr(error,'basic_reason',None) or error.reason or error.code))
                except BrowserError:
                    emit('stage',dict(name='enrichment_discarded',status='skipped',condition='browser_unavailable'))
                return base
    except Exception:
        if observe:after_read_failure(page,same_document,emit)
        raise
    finally:
        try:doc.dispose()
        except BrowserError:pass
