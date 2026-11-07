import {
  HYPERLIQUID_CHART_VERSION,
  HYPERLIQUID_MIN_CHART_HEIGHT,
  HYPERLIQUID_MAX_CHART_ASPECT,
  layoutHyperliquidChartLabels,
  readableHyperliquidChartDimensions,
  selectHyperliquidPriceLevels,
  selectHyperliquidLevelsWithConfig,
  HYPERLIQUID_SELECTION_CONFIG,
  HYPERLIQUID_CAPTURE_FAILURE_CODES,
  hyperliquidPriceWindow,
  type HyperliquidChart,
  type HyperliquidChartSpike,
} from "./collectionHyperliquidChart";

/** Source-chart adapter for CoinGlass's public Hyperliquid chart (module 8526).
 * The reviewed source uses category liquidation prices, Short/Long bar sizes in
 * the selected coin and separate cumulative lines on yAxis 1. No series are
 * reconstructed or changed. The existing source slider focuses the selected
 * price window; getDataURL exports the same re-measured chart. */
export const HYPERLIQUID_CAPTURE_SCRIPT = String.raw`(async () => {
 const finish = result => JSON.stringify({stage:'hyperliquid_chart',...result});
 const fail = reason => ({ok:false,reason});
 const number = value => typeof value === 'number' && Number.isFinite(value);
 const select = (${selectHyperliquidLevelsWithConfig.toString()});
 const selectionConfig = ${JSON.stringify(HYPERLIQUID_SELECTION_CONFIG)};
 const priceWindow = (${hyperliquidPriceWindow.toString()});
 const readable = (width,height) => height >= ${HYPERLIQUID_MIN_CHART_HEIGHT} && width / height <= ${HYPERLIQUID_MAX_CHART_ASPECT};
 const numeric = value => number(value) ? value : typeof value === 'string' && /^(?:\d+(?:\.\d+)?|\.\d+)$/.test(value) ? Number(value) : NaN;
 const name = value => typeof value === 'string' ? value.trim().replace(/\s+/g,' ').toLowerCase() : '';
 const visible = element => {
  if (!element || element.isConnected !== true || typeof element.getBoundingClientRect !== 'function') return false;
  const rect = element.getBoundingClientRect();
  if (!(rect.width > 0 && rect.height > 0)) return false;
  for (let node = element; node; node = node.parentElement) {
   const style = getComputedStyle(node);
   if (!style || style.display === 'none' || style.visibility === 'hidden' || style.visibility === 'collapse' || Number(style.opacity) === 0 && style.opacity !== '') return false;
  }
  return true;
 };
 const gated = () => [...document.querySelectorAll('[role="dialog"],.MuiModal-root')].some(element => visible(element) && /\blog\s*in\s+to\s+unlock\s+full\s+data\b/i.test(element.innerText || ''));
 const instance = element => {
  if (window.echarts && typeof window.echarts.getInstanceByDom === 'function') {
   const chart = window.echarts.getInstanceByDom(element);
   if (chart) return chart;
  }
  // Only the DOM-attached chart wrapper's documented getter is inspected.
  // No account/application state, hook values or network data are read.
  const key = Object.keys(element).find(key => key.startsWith('__reactFiber$') || key.startsWith('__reactInternalInstance$'));
  let fiber = key && element[key];
  for (let count = 0; fiber && count < 32; count++, fiber = fiber.return) {
   const wrapper = fiber.stateNode;
   if (wrapper && wrapper.ele === element && wrapper.echarts && typeof wrapper.echarts.getInstanceByDom === 'function') {
    const chart = wrapper.echarts.getInstanceByDom(element);
    if (chart && typeof chart.getDom === 'function' && chart.getDom() === element) return chart;
   }
  }
  return null;
 };
 const read = () => {
  const page = new URL(location.href);
  if (page.protocol !== 'https:' || !['www.coinglass.com','coinglass.com'].includes(page.hostname) || page.pathname.replace(/\/$/,'') !== '/hyperliquid-liquidation-map' || page.search || page.hash) return fail('wrong_source');
  if (gated()) return fail('source_login_required');
  const candidates = [];
  const mountedCharts = [...document.querySelectorAll('[_echarts_instance_]')];
  for (const element of mountedCharts) {
   if (!visible(element)) continue;
   const chart = instance(element);
   if (!chart || typeof chart.getOption !== 'function' || typeof chart.getDom !== 'function' || chart.getDom() !== element || typeof chart.isDisposed === 'function' && chart.isDisposed()) continue;
   const option = chart.getOption();
   if (!option || !Array.isArray(option.series)) continue;
   const longs = option.series.map((series,index) => ({series,index})).filter(({series}) => series.type === 'bar' && name(series.name) === 'long');
   const shorts = option.series.map((series,index) => ({series,index})).filter(({series}) => series.type === 'bar' && name(series.name) === 'short');
   if (longs.length === 1 && shorts.length === 1) candidates.push({chart,element,option,long:longs[0],short:shorts[0]});
  }
  if (candidates.length !== 1) return fail(candidates.length ? 'ambiguous_chart' : mountedCharts.length ? 'chart_api_unavailable' : 'source_chart_unmounted');
  const {chart,element,option,long,short} = candidates[0];
  // The source module's coin selector defines the bar/tooltip quantity unit.
  let card = element, verifiedCard = false;
  for (let depth = 0; card && depth < 12; depth++, card = card.parentElement) {
   const headings = [...card.querySelectorAll('h1')].filter(node => visible(node) && name(node.innerText) === 'hyperliquid liquidation map');
   const controls = [...card.querySelectorAll('[role="combobox"]')].filter(visible);
   if (headings.length === 1 && controls.length === 1) { verifiedCard = true; break; }
  }
  if (!card || !verifiedCard) return fail('unverified_symbol');
  const controls = [...card.querySelectorAll('[role="combobox"]')].filter(visible);
  if (controls.length !== 1 || controls[0].value !== 'BTC') return fail('unverified_symbol');
  // CoinGlass keeps its inactive spinner mounted. Scrape rendering can omit
  // the CSS rules that consume the overlay's explicit inactive-state variables.
  if ([...card.querySelectorAll('[role="progressbar"],.MuiCircularProgress-root')].some(indicator=>{
   if (!visible(indicator)) return false;
   const overlay=indicator.parentElement;
   return !(overlay && overlay.style && overlay.style.getPropertyValue('--sx-opacity').trim()==='0' && overlay.style.getPropertyValue('--sx-zIndex').trim()==='-1');
  })) return fail('chart_loading');
  const series = option.series;
  const cumulativeShort = series.filter(item => item.type === 'line' && name(item.name) === 'cumulative short liquidation leverage');
  const cumulativeLong = series.filter(item => item.type === 'line' && name(item.name) === 'cumulative long liquidation leverage');
  if (cumulativeShort.length !== 1 || cumulativeLong.length !== 1 || cumulativeShort[0].yAxisIndex !== 1 || cumulativeLong[0].yAxisIndex !== 1 || series.filter(item => item.type === 'bar').length !== 2) return fail('unverified_series');
  const axes = Array.isArray(option.xAxis) ? option.xAxis : [option.xAxis];
  const axis = axes[0], prices = axis && axis.data;
  if (!axis || axis.type !== 'category' || !Array.isArray(prices) || !prices.length || prices.length > 5000 || prices.some(price => !(numeric(price) > 0))) return fail('invalid_prices');
  const yAxes = Array.isArray(option.yAxis) ? option.yAxis : [option.yAxis];
  if (!yAxes[0] || yAxes[0].type !== 'value') return fail('unverified_series');
  const markers = series.flatMap(item => name(item.name) === '' && item.type === 'line' && item.markLine && Array.isArray(item.markLine.data) ? item.markLine.data : []).filter(marker => name(marker.name) === 'current price');
  if (markers.length !== 1) return fail('unverified_current_price');
  const marker = markers[0], markIndex = marker.xAxis;
  const formatter = marker.label && marker.label.formatter;
  const match = typeof formatter === 'string' && /^\{b\}:(\d+(?:\.\d+)?)$/.exec(formatter);
  if (!Number.isInteger(markIndex) || markIndex < 0 || markIndex >= prices.length || !match || numeric(prices[markIndex]) !== Number(match[1])) return fail('unverified_current_price');
  const observedPrice = numeric(prices[markIndex]);
  const legends = Array.isArray(option.legend) ? option.legend : option.legend ? [option.legend] : [];
  if (legends.some(legend => legend.selected && (legend.selected[long.series.name] === false || legend.selected[short.series.name] === false))) return fail('hidden_series');
  if (typeof chart.getWidth !== 'function' || typeof chart.getHeight !== 'function' || typeof chart.getModel !== 'function' || typeof chart.convertToPixel !== 'function' || typeof chart.getDataURL !== 'function') return fail('chart_api_unavailable');
  const width = chart.getWidth(), height = chart.getHeight();
  if (!number(width) || !number(height) || width < 160 || width > 8000 || height <= 0 || height > 8000) return fail('invalid_geometry');
  const model = chart.getModel();
  if (!model || typeof model.getSeriesByIndex !== 'function') return fail('chart_api_unavailable');
  const bars = [], occupied = new Set();
  const levels = [], sourceOccupied = new Set();
  for (const target of [{...long,positionSide:'long'},{...short,positionSide:'short'}]) {
   if (!Array.isArray(target.series.data) || target.series.data.length > prices.length) return fail('invalid_values');
   for(let rawIndex=0;rawIndex<target.series.data.length;rawIndex++) {
    const raw=target.series.data[rawIndex], amount=raw && typeof raw==='object' && !Array.isArray(raw)?raw.value:raw;
    if(amount==null || amount===0) continue;
    const price=numeric(prices[rawIndex]);
    if(!number(amount)||amount<0||(target.positionSide==='long'?price>=observedPrice:price<=observedPrice)||sourceOccupied.has(rawIndex)) return fail('invalid_values');
    sourceOccupied.add(rawIndex);
    levels.push({price,amountBtc:amount,positionSide:target.positionSide});
   }
  }
  for (const target of [{...long,positionSide:'long'},{...short,positionSide:'short'}]) {
   const source = target.series;
   if ((source.xAxisIndex || 0) !== 0 || (source.yAxisIndex || 0) !== 0 || source.stack !== 'a' || source.coordinateSystem && source.coordinateSystem !== 'cartesian2d' || !Array.isArray(source.data)) return fail('unverified_series');
   const seriesModel = model.getSeriesByIndex(target.index);
   const data = seriesModel && typeof seriesModel.getData === 'function' && seriesModel.getData();
   if (!data || ['count','getRawIndex','getRawDataItem','getItemLayout'].some(method => typeof data[method] !== 'function')) return fail('chart_api_unavailable');
   if (data.count() > 5000) return fail('invalid_values');
   for (let index = 0; index < data.count(); index++) {
    const rawIndex = data.getRawIndex(index);
    if (!Number.isInteger(rawIndex) || rawIndex < 0 || rawIndex >= prices.length) return fail('invalid_values');
    const raw = data.getRawDataItem(index), expected = source.data[rawIndex];
    const rawValue = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw.value : raw;
    const expectedValue = expected && typeof expected === 'object' && !Array.isArray(expected) ? expected.value : expected;
    if (rawValue == null && expectedValue == null) continue;
    if (!number(rawValue) || rawValue < 0 || rawValue !== expectedValue) return fail('invalid_values');
    if (rawValue === 0) continue;
    const price = numeric(prices[rawIndex]);
    if (target.positionSide === 'long' ? price >= observedPrice : price <= observedPrice) return fail('invalid_values');
    if (occupied.has(rawIndex)) return fail('overlapping_bars');
    occupied.add(rawIndex);
    const rectangle = data.getItemLayout(index);
    if (!rectangle || ![rectangle.x,rectangle.y,rectangle.width,rectangle.height].every(number) || !(Math.abs(rectangle.width) > 0 && Math.abs(rectangle.height) > 0)) return fail('invalid_geometry');
    const x = rectangle.x + rectangle.width / 2, y = Math.min(rectangle.y,rectangle.y + rectangle.height);
    const point = chart.convertToPixel({seriesIndex:target.index},[rawIndex,rawValue]);
    if (!Array.isArray(point) || point.length !== 2 || !point.every(number) || Math.abs(point[0] - x) > 1 || Math.abs(point[1] - y) > 1) return fail('invalid_geometry');
    if (x < 0 || x > width || y < 0 || y > height || typeof chart.containPixel === 'function' && !chart.containPixel({seriesIndex:target.index},[x,y])) continue;
    bars.push({price,amountBtc:rawValue,positionSide:target.positionSide,x,y});
   }
  }
  if (!bars.length) return fail('empty_chart');
  // getOption can omit the slider's default type. Verify its real existing
  // component instead of inferring a slider from show:true or missing options.
  const zoomTypes = Array.isArray(option.dataZoom) ? option.dataZoom.map((item,index) => {
   if (!item || typeof model.getComponent !== 'function') return null;
   const component = model.getComponent('dataZoom',index);
   const kind = component && component.subType;
   if (!component || component.componentIndex !== index || !['slider','inside'].includes(kind) || component.type !== 'dataZoom.' + kind || item.type != null && item.type !== kind) return null;
   return kind;
  }) : [];
  const sourceSignature = JSON.stringify({prices,observedPrice,series:series.map(item => ({type:item.type,name:item.name,xAxisIndex:item.xAxisIndex || 0,yAxisIndex:item.yAxisIndex || 0,stack:item.stack,data:item.data})),selected:legends.map(legend => legend.selected),marker});
  const dataSignature = JSON.stringify({sourceSignature,dataZoom:option.dataZoom,zoomTypes});
  return {ok:true,chart,element,width,height,observedPrice,bars,levels,prices:prices.map(numeric),dataZoom:option.dataZoom,zoomTypes,sourceSignature,dataSignature};
 };
 try {
  let current;
  for (let attempt = 0; attempt < 25; attempt++) {
   current = read();
   if (current.ok || !['chart_api_unavailable','source_chart_unmounted','chart_loading'].includes(current.reason)) break;
   await new Promise(resolve => setTimeout(resolve,600));
  }
  if (!current || !current.ok) return finish(current || fail('chart_api_unavailable'));
  const original = current;
  const selected = select(current.levels,current.observedPrice,selectionConfig);
  const zoom = priceWindow(selected,current.observedPrice,current.prices);
  const zooms = current.dataZoom;
  if(!zoom || !Array.isArray(zooms) || !zooms.length || zooms.length>8 || !zooms.some((item,index)=>current.zoomTypes[index]==='slider' && item.show!==false) || typeof current.chart.dispatchAction!=='function') return finish(fail('zoom_unavailable'));
  for(let index=0;index<zooms.length;index++) {
   const item=zooms[index], axis=item.xAxisIndex==null?[0]:Array.isArray(item.xAxisIndex)?item.xAxisIndex:[item.xAxisIndex];
   if(!['slider','inside'].includes(current.zoomTypes[index]) || axis.length!==1 || axis[0]!==0) return finish(fail('zoom_unavailable'));
   current.chart.dispatchAction({type:'dataZoom',dataZoomIndex:index,startValue:zoom.startIndex,endValue:zoom.endIndex});
  }
  await new Promise(resolve=>setTimeout(resolve,400));
  current=read();
  if(!current.ok) return finish(current);
  if(current.chart!==original.chart || current.sourceSignature!==original.sourceSignature || JSON.stringify(current.zoomTypes)!==JSON.stringify(original.zoomTypes)) return finish(fail('stale_chart'));
  if(!Array.isArray(current.dataZoom) || current.dataZoom.length!==zooms.length || current.dataZoom.some(item=>numeric(item.startValue)!==zoom.startIndex || numeric(item.endValue)!==zoom.endIndex)) return finish(fail('unverified_zoom'));
  if (!readable(current.width,current.height)) {
   if (typeof current.chart.resize !== 'function' || !current.element.style) return finish(fail('unreadable_geometry'));
   const before = current;
   const height = Math.max(600,Math.ceil(current.width / ${HYPERLIQUID_MAX_CHART_ASPECT}));
   current.element.style.height = height + 'px';
   current.element.style.minHeight = height + 'px';
   try { current.chart.resize({height,animation:{duration:0}}); }
   catch { return finish(fail('unreadable_geometry')); }
   await new Promise(resolve => setTimeout(resolve,100));
   current = read();
   if (!current.ok) return finish(current);
   if (current.chart !== before.chart || current.dataSignature !== before.dataSignature) return finish(fail('stale_chart'));
   if (!readable(current.width,current.height)) return finish(fail('unreadable_geometry'));
  }
  const image = current.chart.getDataURL({type:'png',pixelRatio:2});
  const after = read();
  const key = capture => JSON.stringify({width:capture.width,height:capture.height,observedPrice:capture.observedPrice,bars:capture.bars});
  if (!after.ok || after.chart !== current.chart || after.dataSignature !== current.dataSignature || key(after) !== key(current)) return finish(fail('stale_chart'));
  return finish({ok:true,sourceUrl:'https://www.coinglass.com/hyperliquid-liquidation-map',symbol:'BTC',unit:'BTC',quantityKind:'liquidation_bucket',coordinateSource:'rendered_bar',width:current.width,height:current.height,observedPrice:current.observedPrice,bars:current.bars,selectionLevels:current.levels,priceCategories:current.prices,zoom,image,pixelRatio:2,capturedAt:new Date().toISOString()});
 } catch { return finish(fail('chart_api_unavailable')); }
})()`;

export class HyperliquidChartCaptureError extends Error {
  constructor(public readonly code: string) {
    super(
      code === "source_login_required"
        ? "צילום מפת היפרליקוויד דורש התחברות למקור."
        : code === "source_chart_unmounted"
          ? "גרף קוינגלאס לא הוצג בדף הסריקה."
          : "לא ניתן לאמת צילום של מפת היפרליקוויד כרגע.",
    );
    this.name = "HyperliquidChartCaptureError";
  }
}

const safeReasons = new Set<string>(HYPERLIQUID_CAPTURE_FAILURE_CODES);

export function verifiedHyperliquidCapture(
  returns: unknown[],
  now = Date.now(),
): { image: string; chart: HyperliquidChart } {
  const parsed = returns.map((raw) => {
    const value =
      raw && typeof raw === "object" && "value" in raw ? (raw as { value: unknown }).value : raw;
    try {
      return typeof value === "string" ? JSON.parse(value) : value;
    } catch {
      return null;
    }
  });
  const captures = parsed.filter((raw) => raw?.stage === "hyperliquid_chart");
  const capture = captures[0];
  const reject = (): never => {
    throw new HyperliquidChartCaptureError(
      safeReasons.has(capture?.reason) ? capture.reason : "unverified_capture",
    );
  };
  if (
    captures.length !== 1 ||
    capture?.ok !== true ||
    capture.sourceUrl !== "https://www.coinglass.com/hyperliquid-liquidation-map" ||
    capture.symbol !== "BTC" ||
    capture.unit !== "BTC" ||
    capture.quantityKind !== "liquidation_bucket" ||
    capture.coordinateSource !== "rendered_bar" ||
    capture.pixelRatio !== 2 ||
    !Number.isFinite(now) ||
    typeof capture.capturedAt !== "string" ||
    !/^\d{4}-\d{2}-\d{2}T/.test(capture.capturedAt) ||
    !Number.isFinite(Date.parse(capture.capturedAt)) ||
    Math.abs(now - Date.parse(capture.capturedAt)) > 5 * 60_000 ||
    typeof capture.width !== "number" ||
    typeof capture.height !== "number" ||
    !readableHyperliquidChartDimensions(capture.width, capture.height) ||
    typeof capture.observedPrice !== "number" ||
    !Number.isFinite(capture.observedPrice) ||
    capture.observedPrice <= 0 ||
    !Array.isArray(capture.bars) ||
    !capture.bars.length ||
    capture.bars.length > 5000 ||
    typeof capture.image !== "string" ||
    capture.image.length > 12 * 1024 * 1024 ||
    !/^data:image\/png;base64,[A-Za-z0-9+/]+={0,2}$/.test(capture.image)
  )
    reject();
  const bars: HyperliquidChartSpike[] = [];
  const identities = new Set<string>();
  for (const bar of capture.bars) {
    if (
      !bar ||
      typeof bar !== "object" ||
      ![bar.price, bar.amountBtc, bar.x, bar.y].every(
        (value) => typeof value === "number" && Number.isFinite(value),
      ) ||
      bar.price <= 0 ||
      bar.amountBtc <= 0 ||
      bar.x < 0 ||
      bar.x > capture.width ||
      bar.y < 0 ||
      bar.y > capture.height ||
      (bar.positionSide !== "long" && bar.positionSide !== "short") ||
      (bar.positionSide === "long"
        ? bar.price >= capture.observedPrice
        : bar.price <= capture.observedPrice)
    )
      reject();
    const key = `${bar.positionSide}:${bar.price}:${bar.x}:${bar.y}`;
    if (identities.has(key)) reject();
    identities.add(key);
    bars.push({
      price: bar.price,
      amountBtc: bar.amountBtc,
      positionSide: bar.positionSide,
      x: bar.x,
      y: bar.y,
    });
  }
  if (
    !Array.isArray(capture.selectionLevels) ||
    !capture.selectionLevels.length ||
    capture.selectionLevels.length > 5000 ||
    !Array.isArray(capture.priceCategories)
  )
    reject();
  for (const level of capture.selectionLevels) {
    if (
      !level ||
      ![level.price, level.amountBtc].every(
        (value) => typeof value === "number" && Number.isFinite(value) && value > 0,
      ) ||
      (level.positionSide !== "long" && level.positionSide !== "short") ||
      (level.positionSide === "long"
        ? level.price >= capture.observedPrice
        : level.price <= capture.observedPrice)
    )
      reject();
  }
  const selected = selectHyperliquidPriceLevels(capture.selectionLevels, capture.observedPrice);
  const zoom = hyperliquidPriceWindow(selected, capture.observedPrice, capture.priceCategories);
  if (!zoom) return reject();
  if (
    !capture.zoom ||
    (Object.keys(zoom) as (keyof typeof zoom)[]).some((key) => capture.zoom[key] !== zoom[key])
  )
    reject();
  const used = new Set<number>();
  const spikes: HyperliquidChartSpike[] = [];
  for (const level of selected) {
    const index = bars.findIndex(
      (bar, index) =>
        !used.has(index) &&
        bar.price === level.price &&
        bar.amountBtc === level.amountBtc &&
        bar.positionSide === level.positionSide,
    );
    if (index < 0) reject();
    used.add(index);
    spikes.push(bars[index]);
  }
  spikes.sort((a, b) => a.x - b.x || a.price - b.price);
  return {
    image: capture.image,
    chart: {
      version: HYPERLIQUID_CHART_VERSION,
      sourceUrl: capture.sourceUrl,
      capturedAt: capture.capturedAt,
      symbol: "BTC",
      unit: "BTC",
      quantityKind: "liquidation_bucket",
      coordinateSource: "rendered_bar",
      observedPrice: capture.observedPrice,
      width: capture.width,
      height: capture.height,
      zoom,
      spikes,
      ...layoutHyperliquidChartLabels(spikes, capture.width, capture.height),
    },
  };
}
