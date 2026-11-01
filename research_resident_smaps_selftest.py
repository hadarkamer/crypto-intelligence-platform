"""Pure fabricated smaps controls. No OS file, producer import or native call."""
import importlib.util
import json
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('resident_smaps_under_test',Path(__file__).with_name('research_resident_smaps.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def record(start=0x1000,size=8,rss=4,perms='rw-p',inode=0,path='',**updates):
    values={key:0 for key in m.FIELDS};values.update(Size=size,Rss=rss,Pss=rss,Private_Dirty=rss,Anonymous=rss,KernelPageSize=4,MMUPageSize=4);values.update(updates)
    end=start+size*1024
    text=f'{start:x}-{end:x} {perms} 00000000 00:00 {inode}'+(' '+path if path else '')+'\n'
    return (text+''.join(f'{key}: {value} kB\n'for key,value in values.items())+'VmFlags: rd wr mr mw me ac sd\n').encode()


def parse(raw,**options):return m.parse_smaps(raw,read_complete=True,**options)


class Tests(unittest.TestCase):
    def test_complete_exact_byte_units(self):
        result=parse(record());self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['candidate']['size_bytes'],8192);self.assertEqual(result['candidate']['rss_bytes'],4096)
        self.assertEqual(result['candidate']['nonresident_bytes'],4096)
    def test_incomplete_read_not_a_complete_file(self):
        result=m.parse_smaps(record());self.assertEqual(result['status'],'PARTIAL')
        self.assertIn('INPUT_COMPLETENESS_UNCONFIRMED',result['reason_codes']);self.assertIsNone(result['candidate']['rss_bytes'])
    def test_empty_unavailable(self):self.assertEqual(parse(b'')['status'],'UNAVAILABLE')
    def test_all_status_variants_have_same_closed_top_keys(self):
        values=[parse(record()),m.parse_smaps(record()),parse(b''),parse(None),parse(record(),max_bytes=10)]
        self.assertTrue(all(set(value)==set(values[0])for value in values))
    def test_missing_final_newline(self):self.assertIn('MISSING_FINAL_NEWLINE',parse(record().rstrip(b'\n'))['reason_codes'])
    def test_nonbytes_never_calls_hooks(self):
        calls=[]
        class Foreign:
            def __len__(self):calls.append(1);raise AssertionError()
        self.assertEqual(parse(Foreign())['status'],'UNAVAILABLE');self.assertEqual(calls,[])
    def test_bool_and_invalid_options(self):
        for options in ({'read_complete':1},{'max_bytes':True},{'max_mappings':True},{'max_lines':0}):
            result=m.parse_smaps(record(),**options);self.assertEqual(result['reason_codes'],['INVALID_OPTIONS'])
    def test_byte_cap(self):self.assertEqual(parse(record(),max_bytes=10)['reason_codes'],['BYTE_BOUND'])
    def test_mapping_cap_has_no_overcount(self):
        result=parse(record()+record(0x5000),max_mappings=1)
        self.assertIn('MAPPING_BOUND',result['reason_codes']);self.assertEqual(result['mapping_count'],1);self.assertIsNone(result['candidate']['size_bytes'])
    def test_line_cap(self):self.assertIn('LINE_BOUND',parse(record(),max_lines=3)['reason_codes'])
    def test_dense_newlines_stop_at_line_cap_without_split_list(self):
        result=parse(b'\n'*(1024*1024),max_lines=2)
        self.assertIn('LINE_BOUND',result['reason_codes']);self.assertEqual(result['covered_mapping_count'],0)
        self.assertNotIn('.splitlines(',Path(m.__file__).read_text())
    def test_long_line_cap(self):self.assertIn('LINE_BYTE_BOUND',parse(record(path='A'*9000))['reason_codes'])
    def test_field_count_cap(self):
        raw=record()+b''.join(f'Unknown{i}: 0\n'.encode()for i in range(65))
        self.assertIn('FIELD_COUNT_BOUND',parse(raw)['reason_codes'])
    def test_duplicate_required_field(self):self.assertIn('DUPLICATE_FIELD',parse(record()+b'Rss: 4 kB\n')['reason_codes'])
    def test_duplicate_unknown_field(self):self.assertIn('DUPLICATE_FIELD',parse(record()+b'Unknown: a\nUnknown: b\n')['reason_codes'])
    def test_missing_required_is_not_zero(self):
        result=parse(record().replace(b'Shared_Hugetlb: 0 kB\n',b''))
        self.assertIn('INCOMPLETE_OR_INVALID_MAPPING',result['reason_codes']);self.assertIsNone(result['candidate']['rss_bytes'])
    def test_optional_missing_fields_remain_unknown(self):
        result=parse(record().replace(b'LazyFree: 0 kB\n',b''))
        self.assertEqual(result['status'],'COMPLETE');self.assertIsNone(result['covered_totals_bytes']['LazyFree']);self.assertEqual(result['field_available_counts']['LazyFree'],0)
    def test_units_rejected(self):self.assertIn('INVALID_FIELD_UNITS',parse(record().replace(b'Rss: 4 kB',b'Rss: 4 MB'))['reason_codes'])
    def test_negative_and_decimal_fields_rejected(self):
        for bad in (b'-4',b'4.0',b'+4'):
            self.assertIn('INVALID_FIELD_UNITS',parse(record().replace(b'Rss: 4 kB',b'Rss: '+bad+b' kB'))['reason_codes'])
    def test_field_overflow(self):self.assertIn('FIELD_VALUE_BOUND',parse(record().replace(b'Rss: 4 kB',b'Rss: 9999999999999999999 kB'))['reason_codes'])
    def test_size_must_match_range(self):self.assertIn('INCOHERENT_MAPPING',parse(record().replace(b'Size: 8 kB',b'Size: 9 kB'))['reason_codes'])
    def test_rss_exceeds_size(self):self.assertIn('INCOHERENT_MAPPING',parse(record(rss=9))['reason_codes'])
    def test_components_must_sum_to_rss(self):self.assertIn('INCOHERENT_MAPPING',parse(record(Private_Dirty=3))['reason_codes'])
    def test_pss_and_anonymous_bounds(self):
        for update in ({'Pss':5},{'Anonymous':5}):self.assertIn('INCOHERENT_MAPPING',parse(record(**update))['reason_codes'])
    def test_overlapping_and_nonmonotone_ranges(self):
        self.assertIn('INVALID_OR_OVERLAPPING_RANGE',parse(record()+record(0x2000))['reason_codes'])
        self.assertIn('INVALID_OR_OVERLAPPING_RANGE',parse(record(0x8000)+record())['reason_codes'])
    def test_zero_reversed_range(self):self.assertIn('INVALID_OR_OVERLAPPING_RANGE',parse(record().replace(b'1000-3000',b'3000-1000'))['reason_codes'])
    def test_file_backed_excludes_cow_anonymous_from_candidate(self):
        result=parse(record(inode=1,path='/secret/private/library.so'))
        self.assertEqual(result['classes']['FILE_BACKED']['fields_bytes']['Anonymous'],4096);self.assertEqual(result['candidate']['size_bytes'],0)
    def test_heap_stack_and_named_anonymous_are_broad_candidates(self):
        raw=record(path='[heap]')+record(0x5000,path='[stack]')+record(0x9000,path='[anon:PRIVATE_LABEL]')
        result=parse(raw);self.assertEqual(result['candidate']['mapping_count'],3);self.assertEqual(result['candidate']['size_bytes'],24576)
    def test_prot_none_and_shared_not_rw_private_candidate(self):
        result=parse(record(perms='---p',rss=0)+record(0x5000,perms='rw-s'))
        self.assertEqual(result['candidate']['size_bytes'],0);self.assertEqual(result['classes']['SHARED_ANONYMOUS']['mapping_count'],1)
    def test_executable_rw_private_still_overinclusive(self):self.assertEqual(parse(record(perms='rwxp'))['candidate']['mapping_count'],1)
    def test_hugetlb_disables_ordinary_rss_bound(self):
        result=parse(record(rss=0,Private_Hugetlb=4));self.assertEqual(result['status'],'COMPLETE')
        self.assertTrue(result['hugetlb_present']);self.assertFalse(result['candidate']['ordinary_gauges_complete']);self.assertIsNone(result['candidate']['rss_bytes'])
    def test_hugetlb_and_ordinary_residency_cannot_exceed_mapping(self):
        self.assertIn('INCOHERENT_MAPPING',parse(record(rss=4,Private_Hugetlb=8))['reason_codes'])
    def test_class_sum_matches_whole_projection(self):
        result=parse(record()+record(0x5000,inode=1,path='/fake/file'))
        for key,value in result['covered_totals_bytes'].items():self.assertEqual(sum(v['fields_bytes'][key]for v in result['classes'].values()),value)
    def test_no_raw_addresses_path_or_values_persisted(self):
        raw=record(start=0x12345000,path='[anon:VERY_PRIVATE_TEST_VALUE]');result=parse(raw);text=json.dumps(result)
        for private in ('12345000','VERY_PRIVATE_TEST_VALUE','00:00'):self.assertNotIn(private,text)
        self.assertFalse(result['addresses_or_paths_persisted']);self.assertFalse(result['allocator_containment_established'])
    def test_unknown_new_kernel_fields_accepted_but_not_retained(self):
        result=parse(record()+b'FutureKernelField: secret-payload\n');self.assertEqual(result['status'],'COMPLETE')
        self.assertNotIn('secret-payload',json.dumps(result));self.assertNotIn('FutureKernelField',json.dumps(result))
    def test_malformed_line_and_orphan_field(self):
        self.assertIn('FIELD_WITHOUT_MAPPING',parse(b'Rss: 1 kB\n')['reason_codes'])
        self.assertIn('MALFORMED_MAPPING_LINE',parse(record()+b'not a valid smaps field\n')['reason_codes'])
    def test_page_sizes_preserved_not_summed_as_capacity(self):
        result=parse(record());self.assertEqual(result['page_sizes_bytes'],{'KernelPageSize':[4096],'MMUPageSize':[4096]})
        self.assertNotIn('KernelPageSize',result['covered_totals_bytes'])
    def test_missing_invalid_page_size_and_unaligned_range(self):
        self.assertIn('INCOMPLETE_OR_INVALID_MAPPING',parse(record().replace(b'KernelPageSize: 4 kB\n',b''))['reason_codes'])
        for raw in (record(KernelPageSize=3),record(start=0x1001)):
            self.assertIn('INCOHERENT_MAPPING',parse(raw)['reason_codes'])


if __name__=='__main__':unittest.main()
