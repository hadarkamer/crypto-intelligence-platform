"""Pure bounded smaps projection. No import-time I/O, native calls or probes.

Only kernel mapping gauges are parsed. No address, path, inode, device or raw
line enters the result. Allocator containment is an external assumption.
"""
import re

VERSION='resident-smaps-projection-v1'
MAX_BYTES=2*1024*1024
MAX_MAPPINGS=4096
MAX_LINES=65536
MAX_LINE_BYTES=8192
MAX_FIELDS_PER_MAPPING=64
MAX_VALUE=2**63-1
FIELDS=('Size','Rss','Pss','Shared_Clean','Shared_Dirty','Private_Clean','Private_Dirty',
        'Anonymous','Swap','Shared_Hugetlb','Private_Hugetlb','AnonHugePages','LazyFree')
REQUIRED=frozenset(FIELDS[:11])
PAGE_FIELDS=('KernelPageSize','MMUPageSize')
CLASSES=('HEAP','STACK','PRIVATE_RW_ANONYMOUS','ANONYMOUS_OTHER','SHARED_ANONYMOUS','FILE_BACKED','SPECIAL_OTHER')
_HEADER=re.compile(rb'([0-9a-fA-F]{1,16})-([0-9a-fA-F]{1,16})[ \t]+([r-][w-][x-][ps])[ \t]+([0-9a-fA-F]{1,16})[ \t]+([0-9a-fA-F]{1,8}):([0-9a-fA-F]{1,8})[ \t]+([0-9]{1,20})(?:[ \t]+(.*))?')
_KEY=re.compile(rb'([A-Za-z_][A-Za-z_0-9]*):[ \t]*(.*)')
_NUMBER=re.compile(rb'([0-9]{1,19})[ \t]+kB')


def _empty(status,codes,input_bytes=None,read_complete=False):
    return {'version':VERSION,'status':status,'reason_codes':sorted(set(codes)),
        'input_bytes':input_bytes,'read_complete':read_complete,'mapping_count':0,
        'covered_mapping_count':0,'classes':{},'covered_totals_bytes':{},'field_available_counts':{},'page_sizes_bytes':{},
        'candidate':{'mapping_count':0,'covered_size_bytes':0,'covered_rss_bytes':0,
            'size_bytes':None,'rss_bytes':None,'nonresident_bytes':None,
            'ordinary_gauges_complete':False},'hugetlb_present':None,
        'addresses_or_paths_persisted':False,'raw_mapping_payload_persisted':False,
        'allocator_containment_established':False,'atomic':False,
        'scope':'KERNEL_MAPPING_GAUGES_NOT_ALLOCATOR_OWNERSHIP_FREE_PAGES_OR_EXCLUSIVE_PHYSICAL_FRAMES',
        'limits':{'bytes':MAX_BYTES,'mappings':MAX_MAPPINGS,'lines':MAX_LINES,
            'line_bytes':MAX_LINE_BYTES,'fields_per_mapping':MAX_FIELDS_PER_MAPPING},
        'interval_scope':'Only inputs for separately conditional accounting. No free-capacity interval is calculated here.'}


def _classification(perms,inode,path):
    # Names are used only for local classification and are never returned.
    file_backed=inode!=0 or path.startswith((b'/',b'memfd:'))
    private_rw=perms[0:2]==b'rw'and perms[3:4]==b'p'
    anonymous=not file_backed and(not path or path.startswith(b'['))
    candidate=private_rw and anonymous
    if path==b'[heap]':kind='HEAP'
    elif path==b'[stack]'or path.startswith(b'[stack:'):kind='STACK'
    elif file_backed:kind='FILE_BACKED'
    elif anonymous and perms[3:4]==b's':kind='SHARED_ANONYMOUS'
    elif candidate:kind='PRIVATE_RW_ANONYMOUS'
    elif anonymous:kind='ANONYMOUS_OTHER'
    else:kind='SPECIAL_OTHER'
    return kind,candidate


def parse_smaps(raw,*,read_complete=False,max_bytes=MAX_BYTES,max_mappings=MAX_MAPPINGS,max_lines=MAX_LINES):
    """Project supplied bytes only. Caller must establish EOF/read completion.

    A structurally valid prefix cannot prove that a proc file ended. Without an
    explicit complete-read witness, only covered aggregates are returned.
    """
    if type(raw)is not bytes:return _empty('UNAVAILABLE',['INVALID_INPUT_TYPE'])
    if (type(read_complete)is not bool or type(max_bytes)is not int or not 1<=max_bytes<=MAX_BYTES or
        type(max_mappings)is not int or not 1<=max_mappings<=MAX_MAPPINGS or
        type(max_lines)is not int or not 1<=max_lines<=MAX_LINES):return _empty('UNAVAILABLE',['INVALID_OPTIONS'])
    if len(raw)>max_bytes:return _empty('UNAVAILABLE',['BYTE_BOUND'],len(raw),read_complete)
    result=_empty('PARTIAL',[],len(raw),read_complete);codes=[]
    if not read_complete:codes.append('INPUT_COMPLETENESS_UNCONFIRMED')
    if not raw or not raw.endswith(b'\n'):codes.append('MISSING_FINAL_NEWLINE')
    classes={name:{'mapping_count':0,'fields_bytes':{field:0 for field in FIELDS},'field_available_counts':{field:0 for field in FIELDS}}for name in CLASSES}
    totals={field:0 for field in FIELDS};available={field:0 for field in FIELDS};current=None;last_end=None;mapping_count=covered=0
    page_sizes={field:set()for field in PAGE_FIELDS}
    candidate_count=candidate_size=candidate_rss=0;hugetlb=0
    def finish():
        nonlocal covered,candidate_count,candidate_size,candidate_rss,hugetlb
        if current is None:return
        fields=current['fields']
        if current['bad']or not REQUIRED|set(PAGE_FIELDS)<=set(fields):
            codes.append('INCOMPLETE_OR_INVALID_MAPPING');return
        size,rss=fields['Size'],fields['Rss']
        pages=[fields[key]for key in PAGE_FIELDS]
        if (size!=current['size']or rss>size or fields['Pss']>rss or
            rss+fields['Shared_Hugetlb']+fields['Private_Hugetlb']>size or
            sum(fields[key]for key in ('Shared_Clean','Shared_Dirty','Private_Clean','Private_Dirty'))!=rss or
            fields['Anonymous']>rss or any(not 1024<=p<=1024*1024*1024 or p&(p-1)for p in pages)or
            current['start']%min(pages)or current['end']%min(pages)):
            codes.append('INCOHERENT_MAPPING');return
        if any(totals[key]+fields.get(key,0)>MAX_VALUE for key in FIELDS):
            codes.append('AGGREGATE_BOUND');return
        covered+=1;group=classes[current['kind']];group['mapping_count']+=1
        for key in FIELDS:
            value=fields.get(key,0);totals[key]+=value;group['fields_bytes'][key]+=value
            if key in fields:available[key]+=1;group['field_available_counts'][key]+=1
        hugetlb+=fields['Shared_Hugetlb']+fields['Private_Hugetlb']
        for key in PAGE_FIELDS:page_sizes[key].add(fields[key])
        if current['candidate']:
            candidate_count+=1;candidate_size+=size;candidate_rss+=rss
    # Scan one bounded line at a time; a newline-only input must not allocate
    # a list for millions of lines before the declared line cap is enforced.
    offset=line_index=0
    while offset<len(raw):
        end=raw.find(b'\n',offset)
        if end<0:end=len(raw)
        line=raw[offset:end];offset=end+1;line_index+=1
        if line.endswith(b'\r'):line=line[:-1]
        if line_index>max_lines:codes.append('LINE_BOUND');break
        if len(line)>MAX_LINE_BYTES:codes.append('LINE_BYTE_BOUND');break
        if not line:continue
        header=_HEADER.fullmatch(line)
        if header:
            finish()
            if mapping_count>=max_mappings:codes.append('MAPPING_BOUND');current=None;break
            mapping_count+=1
            start,end=int(header[1],16),int(header[2],16);inode=int(header[7]);bad=False
            if start>=end or end-start>MAX_VALUE or inode>2**64-1 or(last_end is not None and start<last_end):codes.append('INVALID_OR_OVERLAPPING_RANGE');bad=True
            last_end=end;kind,candidate=_classification(header[3],inode,header[8]or b'')
            current={'start':start,'end':end,'size':end-start,'kind':kind,'candidate':candidate,'fields':{},'seen_fields':set(),'field_count':0,'bad':bad}
            continue
        if current is None:codes.append('FIELD_WITHOUT_MAPPING');continue
        field=_KEY.fullmatch(line)
        if field is None:codes.append('MALFORMED_MAPPING_LINE');current['bad']=True;continue
        current['field_count']+=1
        if current['field_count']>MAX_FIELDS_PER_MAPPING:codes.append('FIELD_COUNT_BOUND');current['bad']=True;continue
        # Only fixed ASCII keys are decoded; unknown kernel fields are ignored.
        key=field[1].decode('ascii')
        if key in current['seen_fields']:codes.append('DUPLICATE_FIELD');current['bad']=True;continue
        current['seen_fields'].add(key)
        if key not in FIELDS and key not in PAGE_FIELDS:continue
        value=_NUMBER.fullmatch(field[2])
        if value is None:codes.append('INVALID_FIELD_UNITS');current['bad']=True;continue
        count=int(value[1])
        if count>MAX_VALUE//1024:codes.append('FIELD_VALUE_BOUND');current['bad']=True;continue
        current['fields'][key]=count*1024
    finish()
    if not mapping_count:codes.append('NO_MAPPINGS')
    complete=not codes and covered==mapping_count and covered>0
    ordinary=complete and hugetlb==0
    for group in classes.values():
        for key in FIELDS:
            if group['field_available_counts'][key]!=group['mapping_count']:group['fields_bytes'][key]=None
    for key in FIELDS:
        if available[key]!=covered:totals[key]=None
    result.update(status='COMPLETE'if complete else'PARTIAL'if covered else'UNAVAILABLE',reason_codes=sorted(set(codes)),mapping_count=mapping_count,
        covered_mapping_count=covered,classes=classes,covered_totals_bytes=totals,field_available_counts=available,page_sizes_bytes={key:sorted(values)for key,values in page_sizes.items()},hugetlb_present=hugetlb>0 if covered else None,
        candidate={'mapping_count':candidate_count,'covered_size_bytes':candidate_size,'covered_rss_bytes':candidate_rss,
            'size_bytes':candidate_size if ordinary else None,'rss_bytes':candidate_rss if ordinary else None,
            'nonresident_bytes':candidate_size-candidate_rss if ordinary else None,'ordinary_gauges_complete':ordinary})
    return result
