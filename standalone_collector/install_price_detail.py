"""Narrow app-runtime adaptation; original bot and numeric validators unchanged."""
from pathlib import Path

RECT_SCRIPT=r'''() => {
  const canvases=[...document.querySelectorAll('canvas')].map(el=>{
    const r=el.getBoundingClientRect(),s=getComputedStyle(el);
    return {x:r.left+scrollX,y:r.top+scrollY,width:r.width,height:r.height,
      visible:s.display!=='none'&&s.visibility!=='hidden'&&Number(s.opacity||1)>0};
  }).filter(r=>r.visible&&r.width>=400&&r.height>=200);
  canvases.sort((a,b)=>b.width*b.height-a.width*a.height);
  if(!canvases.length)return null;
  const r=canvases[0];
  return {x:r.x,y:r.y,width:r.width,height:r.height,
    page_width:Math.max(document.documentElement.scrollWidth,innerWidth)};
}'''


def replace_once(text,old,new):
    if text.count(old)!=1:raise RuntimeError('Price-detail source layout changed')
    return text.replace(old,new,1)


def expand_detail_capture(text):
    text=replace_once(text,'            page.screenshot(path=str(path), full_page=True)',
        '            chart_rect = page.evaluate('+repr(RECT_SCRIPT)+')\n'
        '            page.screenshot(path=str(path), full_page=True)')
    return replace_once(text,'                    "liquidity_threshold": 0.85,',
        '                    "liquidity_threshold": 0.85,\n'
        '                    "price_geometry": chart_rect,')


def install(runtime:Path):
    path=runtime/'market_vision/coinglass_heatmap_capture.py'
    text=expand_detail_capture(path.read_text());compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
    path=runtime/'market_vision/openai_heatmap_scanner.py';text=path.read_text()
    text=replace_once(text,'import requests','import requests\nfrom price_detail_input import detail_content')
    text=replace_once(text,'        count += 1','        content.extend(detail_content(image))\n        count += 1')
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
    path=runtime/'collection_model1_task.py';text=path.read_text()
    text=replace_once(text,'    images[0].pop("liquidity_threshold", None)',
        '    images[0].pop("liquidity_threshold", None)\n'
        '    if "price_geometry" in images[0]:\n'
        '        from price_detail_input import make_detail_file\n'
        '        images[0]["price_detail"] = make_detail_file(\n'
        '            images[0]["image"], images[0]["price_geometry"])')
    needle='    result = normalize(raw, timeframe=timeframe, run_id=run_id, captured_at=captured_at, image=image)'
    text=replace_once(text,needle,needle+'\n'
        '    from price_detail_input import detail_provenance\n'
        '    price_detail = detail_provenance(images[0])\n'
        '    if price_detail is not None:\n'
        '        result["image_preprocessing"] = price_detail')
    compile(text,str(path),'exec');path.write_text(text,encoding='utf-8')
