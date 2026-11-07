/** Deterministic chart actions, never JavaScript supplied by a worksheet or AI.
 * Unsupported chart engines and ambiguous data fail before a screenshot is saved. */
const ETF_CHART_HELPERS = `
 const failure = reason => ({ok:false,reason});
 const exactName = name => typeof name === 'string' && name.trim().replace(/\\s+/g,' ').toLowerCase() === 'daily total net inflow';
 const validDate = label => typeof label === 'string' && /^\\d{4}-\\d{2}-\\d{2}$/.test(label) && Number.isFinite(Date.parse(label+'T00:00:00.000Z')) && new Date(label+'T00:00:00.000Z').toISOString().slice(0,10) === label;
 const units = axis => {
  if (!axis || axis.type !== 'value' || typeof axis.name !== 'string') return null;
  const formatter = axis.axisLabel && axis.axisLabel.formatter;
  if (formatter != null && formatter !== '{value}') return null;
  const name = axis.name.trim().replace(/\\s+/g,' ').toLowerCase();
  if (name === 'usd' || name === 'us dollars') return 'USD';
  if (/^(?:usd\\s*\\(\\s*millions\\s*\\)|usd millions|musd|usd_millions)$/.test(name)) return 'USD_MILLIONS';
  return null;
 };
 const visible = (chart,element) => {
  if (!element || element.isConnected !== true || typeof element.getBoundingClientRect !== 'function' || typeof chart.getWidth !== 'function' || typeof chart.getHeight !== 'function' || (typeof chart.isDisposed === 'function' && chart.isDisposed())) return false;
  const rect = element.getBoundingClientRect();
  if (typeof rect.right === 'number' && typeof rect.bottom === 'number' && (rect.right <= 0 || rect.bottom <= 0 || typeof innerWidth === 'number' && rect.left >= innerWidth || typeof innerHeight === 'number' && rect.top >= innerHeight)) return false;
  if (![rect.width,rect.height,chart.getWidth(),chart.getHeight()].every(value => typeof value === 'number' && Number.isFinite(value) && value > 0)) return false;
  const styleFor = typeof getComputedStyle === 'function' ? getComputedStyle : window.getComputedStyle;
  if (typeof styleFor !== 'function') return false;
  for (let node = element; node; node = node.parentElement) {
   const style = styleFor(node);
   if (!style || style.display === 'none' || style.visibility === 'hidden' || style.visibility === 'collapse' || (style.opacity !== '' && Number(style.opacity) === 0)) return false;
  }
  return true;
 };
 const axisIndex = value => value === undefined ? 0 : Number.isInteger(value) && value >= 0 ? value : -1;
 const zoomAxes = zoom => {
  if (zoom.xAxisIndex === undefined) return zoom.yAxisIndex === undefined ? [0] : [];
  return Array.isArray(zoom.xAxisIndex) ? zoom.xAxisIndex : [zoom.xAxisIndex];
 };
 const readCandidate = () => {
  const instance = element => {
   if (window.echarts && typeof window.echarts.getInstanceByDom === 'function') {
    const chart=window.echarts.getInstanceByDom(element); if(chart) return chart;
   }
   const key=Object.keys(element).find(key=>key.startsWith('__reactFiber$')||key.startsWith('__reactInternalInstance$'));
   let fiber=key&&element[key];
   for(let count=0;fiber&&count<32;count++,fiber=fiber.return){
    const wrapper=fiber.stateNode;
    if(wrapper&&wrapper.ele===element&&wrapper.echarts&&typeof wrapper.echarts.getInstanceByDom==='function'){
     const chart=wrapper.echarts.getInstanceByDom(element); if(chart&&typeof chart.getDom==='function'&&chart.getDom()===element) return chart;
    }
   }
   return null;
  };
  const matches = [];
  let hidden = false;
  for (const element of document.querySelectorAll('[_echarts_instance_]')) {
   const chart = instance(element);
   if (!chart || typeof chart.getOption !== 'function') continue;
   const option = chart.getOption();
   if (!option || !Array.isArray(option.series)) continue;
   for (let seriesIndex = 0; seriesIndex < option.series.length; seriesIndex++) {
    const series = option.series[seriesIndex];
    if (!series || !exactName(series.name)) continue;
    const legends = Array.isArray(option.legend) ? option.legend : option.legend ? [option.legend] : [];
    if (!visible(chart,element) || legends.some(legend => legend.selected && legend.selected[series.name] === false)) { hidden = true; continue; }
    matches.push({chart,element,option,series,seriesIndex});
   }
  }
  if (matches.length !== 1) return failure(matches.length > 1 ? 'ambiguous_series' : hidden ? 'hidden_chart' : 'inflow_series_unavailable');
  const result = matches[0], series = result.series, option = result.option;
  if (series.type !== 'bar' || (series.stack != null && series.stack !== '') || series.coordinateSystem && series.coordinateSystem !== 'cartesian2d') return failure('unsupported_series');
  const xAxisIndex = axisIndex(series.xAxisIndex), yAxisIndex = axisIndex(series.yAxisIndex);
  const axes = Array.isArray(option.xAxis) ? option.xAxis : option.xAxis ? [option.xAxis] : [];
  const yAxes = Array.isArray(option.yAxis) ? option.yAxis : option.yAxis ? [option.yAxis] : [];
  const axis = axes[xAxisIndex], unit = units(yAxes[yAxisIndex]);
  if (!unit) return failure('unknown_units');
  const labels = axis && axis.data, values = series.data;
  if (!axis || axis.type !== 'category' || !Array.isArray(labels) || !Array.isArray(values) || labels.length !== values.length || labels.length < 21) return failure('invalid_values');
  for (let index = 0; index < labels.length; index++) if (!validDate(labels[index]) || index > 0 && labels[index] <= labels[index-1]) return failure('invalid_dates');
  const last = labels.length-1, first = last-20;
  const tailLabels = labels.slice(first), tailValues = values.slice(first);
  for (let index = 0; index < 21; index++) if (typeof tailValues[index] !== 'number' || !Number.isFinite(tailValues[index])) return failure('invalid_values');
  const amount = unit === 'USD' ? tailValues[20]/1000000 : tailValues[20];
  if (!Number.isFinite(amount)) return failure('invalid_values');
  return {...result,ok:true,xAxisIndex,yAxisIndex,unit,first,last,labels:tailLabels,values:tailValues,total:labels.length,amount};
 };
`;

export const ETF_PREPARE_SCRIPT = `(() => {
 ${ETF_CHART_HELPERS}
 const finish = result => JSON.stringify({stage:'etf_prepare',...result});
 window.__collectionEtfCapture = null;
 window.__collectionEtfVerified = false;
 try {
  const current = readCandidate();
  if (!current.ok) return finish(current);
  if (typeof current.chart.setOption !== 'function' || typeof current.chart.dispatchAction !== 'function') return finish(failure('chart_api_unavailable'));
  const existing = Array.isArray(current.option.dataZoom) ? current.option.dataZoom : [];
  if (existing.some(zoom => zoomAxes(zoom).includes(current.xAxisIndex) && zoomAxes(zoom).length !== 1)) return finish(failure('ambiguous_axis'));
  const target = {start:undefined,end:undefined,startValue:current.labels[0],endValue:current.labels[20],filterMode:'filter'};
  let hasTarget = false;
  const zooms = existing.map((zoom,index) => {
   if (!zoomAxes(zoom).includes(current.xAxisIndex)) return zoom;
   hasTarget = true;
   return {...zoom,...target,id:zoom.id || '__collection_etf_zoom_'+index};
  });
  if (!hasTarget) zooms.push({id:'__collection_etf_inside',type:'inside',xAxisIndex:current.xAxisIndex,...target},{id:'__collection_etf_slider',type:'slider',xAxisIndex:current.xAxisIndex,...target});
  current.chart.setOption({animation:false,dataZoom:zooms},{replaceMerge:['dataZoom']});
  for (const zoom of zooms) if (zoomAxes(zoom).includes(current.xAxisIndex)) current.chart.dispatchAction({type:'dataZoom',dataZoomId:zoom.id,startValue:current.labels[0],endValue:current.labels[20]});
  window.__collectionEtfCapture = {chart:current.chart,element:current.element,seriesIndex:current.seriesIndex,xAxisIndex:current.xAxisIndex,yAxisIndex:current.yAxisIndex,unit:current.unit,first:current.first,last:current.last,total:current.total,labels:[...current.labels],values:[...current.values]};
  return finish({ok:true});
 } catch { return finish(failure('chart_api_unavailable')); }
})()`;

export const ETF_VERIFY_SCRIPT = `(() => {
 ${ETF_CHART_HELPERS}
 const finish = result => JSON.stringify({stage:'etf_verify',...result});
 try {
  const state = window.__collectionEtfCapture;
  if (!state) return finish(failure('capture_not_prepared'));
  const current = readCandidate();
  if (!current.ok) return finish(current);
  if (current.chart !== state.chart || current.element !== state.element || current.seriesIndex !== state.seriesIndex || current.xAxisIndex !== state.xAxisIndex || current.yAxisIndex !== state.yAxisIndex || current.unit !== state.unit || current.first !== state.first || current.last !== state.last || current.total !== state.total || current.labels.some((label,index) => label !== state.labels[index]) || current.values.some((value,index) => value !== state.values[index])) return finish(failure('stale_data'));
  const zooms = (Array.isArray(current.option.dataZoom) ? current.option.dataZoom : []).filter(zoom => zoomAxes(zoom).includes(current.xAxisIndex));
  const position = value => typeof value === 'number' && Number.isInteger(value) ? value : value === current.labels[0] ? current.first : value === current.labels[20] ? current.last : -1;
  if (!zooms.length || zooms.some(zoom => zoomAxes(zoom).length !== 1 || position(zoom.startValue) !== current.first || position(zoom.endValue) !== current.last || zoom.filterMode !== 'filter')) return finish(failure('unverified_window'));
  if (typeof current.chart.getModel !== 'function' || typeof current.chart.getDataURL !== 'function') return finish(failure('chart_api_unavailable'));
  const model = current.chart.getModel();
  const seriesModel = model && typeof model.getSeriesByIndex === 'function' && model.getSeriesByIndex(current.seriesIndex);
  const rendered = seriesModel && typeof seriesModel.getData === 'function' && seriesModel.getData();
  if (!rendered || typeof rendered.count !== 'function' || typeof rendered.getRawIndex !== 'function' || typeof rendered.getRawDataItem !== 'function') return finish(failure('chart_api_unavailable'));
  if (rendered.count() !== 21) return finish(failure('unverified_window'));
  for (let index = 0; index < 21; index++) if (rendered.getRawIndex(index) !== current.first+index || rendered.getRawDataItem(index) !== current.values[index]) return finish(failure('unverified_window'));
  const image = current.chart.getDataURL({type:'png',pixelRatio:2,backgroundColor:'#fff'});
  window.__collectionEtfVerified = true;
  return finish({ok:true,visible_candles:21,rendered_candles:21,window_start_index:current.first,window_end_index:current.last,total_points:current.total,image,labels:current.labels,values:current.values,series_name:current.series.name,unit:current.unit,last_amount_musd:current.amount,last_date:current.labels[20]});
 } catch { return finish(failure('chart_api_unavailable')); }
})()`;

const safeEtfReasons = new Set([
  "chart_api_unavailable",
  "inflow_series_unavailable",
  "ambiguous_series",
  "hidden_chart",
  "unsupported_series",
  "unknown_units",
  "invalid_values",
  "invalid_dates",
  "ambiguous_axis",
  "capture_not_prepared",
  "stale_data",
  "unverified_window",
]);

export function verifiedEtfCapture(returns: unknown[]): {
  image: string;
  visibleCandles: 21;
  data: Record<string, unknown>;
  latestInflowMusd: number;
  latestDate: string;
} {
  const parsed = returns.map((raw) => {
    const value =
      raw && typeof raw === "object" && "value" in raw ? (raw as { value: unknown }).value : raw;
    try {
      return typeof value === "string" ? JSON.parse(value) : value;
    } catch {
      return null;
    }
  });
  const prepares = parsed.filter((entry) => entry?.stage === "etf_prepare");
  const verifies = parsed.filter((entry) => entry?.stage === "etf_verify");
  const prepare = prepares[0],
    verify = verifies[0];
  const reject = (): never => {
    const reason = [prepare, verify].find(
      (entry) => entry?.ok !== true && safeEtfReasons.has(entry?.reason),
    )?.reason;
    throw new Error(`לא אומת צילום של 21 נרות ETF.${reason ? ` (${reason})` : ""}`);
  };
  if (
    prepares.length !== 1 ||
    verifies.length !== 1 ||
    prepare?.ok !== true ||
    verify?.ok !== true ||
    verify.visible_candles !== 21 ||
    verify.rendered_candles !== 21 ||
    !Array.isArray(verify.labels) ||
    verify.labels.length !== 21 ||
    !Array.isArray(verify.values) ||
    verify.values.length !== 21 ||
    typeof verify.series_name !== "string" ||
    verify.series_name.trim().replace(/\s+/g, " ").toLowerCase() !== "daily total net inflow" ||
    (verify.unit !== "USD" && verify.unit !== "USD_MILLIONS") ||
    !Number.isInteger(verify.window_start_index) ||
    verify.window_start_index < 0 ||
    !Number.isInteger(verify.window_end_index) ||
    verify.window_end_index - verify.window_start_index !== 20 ||
    !Number.isInteger(verify.total_points) ||
    verify.total_points !== verify.window_end_index + 1 ||
    typeof verify.image !== "string" ||
    !/^data:image\/png;base64,[A-Za-z0-9+/]+={0,2}$/.test(verify.image)
  )
    reject();
  for (let index = 0; index < 21; index++) {
    const label = verify.labels[index],
      value = verify.values[index];
    if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      typeof label !== "string" ||
      !/^\d{4}-\d{2}-\d{2}$/.test(label) ||
      !Number.isFinite(Date.parse(`${label}T00:00:00.000Z`)) ||
      new Date(`${label}T00:00:00.000Z`).toISOString().slice(0, 10) !== label ||
      (index > 0 && label <= verify.labels[index - 1])
    )
      reject();
  }
  const latestDate: string = verify.labels[20];
  const latestInflowMusd: number =
    verify.unit === "USD" ? verify.values[20] / 1_000_000 : verify.values[20];
  if (
    !Number.isFinite(latestInflowMusd) ||
    verify.last_amount_musd !== latestInflowMusd ||
    verify.last_date !== latestDate
  )
    reject();
  const { image, ...data } = verify;
  return { image, visibleCandles: 21, data, latestInflowMusd, latestDate };
}
