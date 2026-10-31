"""Prove new passive Watch hooks erase to the deployed86e orchestration.

Only stdlib AST/source inspection runs here; bot startup and provider modules
are not imported. Runtime helper failure/concurrency tests are separate.
"""
from __future__ import annotations

import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent
MODULE = 'research_watch_runtime'
HANDLE = 'runtime_observation_handle'
PHASES = {'DERIVATIVES_READY', 'SCORING_READY', 'INPUTS_READY', 'BEFORE_DELIVERY'}
BASELINE_MAIN_AST = '919abe1fc6ab4389737cef3b5ea0030a32ceee1c33e43d970520b70c3957c9df'
NATIVE_SOURCE = {
    'fmt': '0df0dc0277db8c0621f5edda239a97faf1acd293a38191eb4f5178ce39f44cce',
    'fmt_price': '2fdd79726a32faaee0bd260b8ed354ca4f5d4c9647453b46543995572872cab5',
    '_flow_direction_icon': 'fd1d4f705a32c9198da5bcfbe336a5c921630367c454c463b558ffb562b9ccc6',
    '_direction_display_he': 'e77de6747c84b561e3309c107c130446213b40f3fd050520b1ba8b23ffacb637',
    '_display_text_he': 'dfd1216835c8cbd22360fce14364693f03dadd1cb62ec60e06a7e0c9b589c8db',
    '_confirmation_state_key': 'bf237b81e1ef6a7676fb489f712df2caee86f1de062381d3bc88f0cae6432d99',
    '_confirmation_transition_message': 'd4eb224077411ff307e5e515a8ed3d809f6d4cf47a572d5d314abd812d82be03',
    '_score_confirmation_transition_message': '6c1d0a58b3f3dc0314f3a97ba3026da2178a1567825c62dbf4cb25170a5498c9',
    '_high_score_83_transition_message': '2d7134bf3a18d8a1be1b298ba01a311beaeb96b322e9a6c8e24fea0070586941',
    '_special_transition_messages': '9ada179bfa648ef5963c9e65b1d0882fc32f1dd23b74c8bef2a427363240da9e',
    '_derivatives_high_transition_messages': '62efad0f60ea4ded7908f05d1ec8f13c23e0ea3ce813e7d57a5cdde8adca3bfe',
    '_spot_family_high_transition_messages': '830b85868fed9c7719e98459001f6c249708063b3afbe638a002568fe5c70d12',
    '_collect_special_transition_messages': 'ba09981842803c3c545227025d28ac07ded8a84ec86f185e7aba5fafa4dbe9ad',
    '_combined_confirmation_message': '7e7b8cf24ed1a71c1f8081f899189717b5ddc0ab985ec6bb980f5ebbb5109573',
    '_collect_combined_confirmation_messages': '0dac152e6d751b86c8667d72959e93571292cb4ea02dbaef24fba4889bac364d',
    '_next_aligned_watch_time': '6f072c4e0fb82523b6eb7fc700b80f2bc1c862387ae97c88cec8491cf07a29b8',
    '_claim_watch_slot': 'da60435447e54d5bf27e485ffd2fb222486110804e0fe9f065f10b905e9e05dd',
    'watch_loop': '9200b07f8e67eb14f547978d127323a03800e9b8081b6f4aedc12ab4747453ee',
    '_watch_supervisor_loop': '2b53d2b0a1d6885379add14257413aefde8fccf4b226da9811011e3d728656fb',
    '_combined_confirmation_candidates': '5a2c12f05e5bf06bb3b0fa65f4b0f5da52a8a655bda7fe04ed56b17f013e59ad',
    'collect_live_rows_for_watch': 'ada4e2a98f6e6d4c531a6491c21218ce1c67008a0b7839e44002a337a899c2a9',
}
FIXTURE_METHODS = {
    'test_unqualified_group_and_exact_thresholds_keep_actual_top_item': 'cd69f05b10b9598a37eec25e7d8010c383bd482412647659c3faeb1d35bb0e9f',
    'test_magnet_sink_retains_negative_missing_error_and_unchanged_best_rank': 'f928c6db987528ad1d80b720bdc3260f71150597c5b4b36a60bd623a47e6f657',
    'test_cached_candidates_preserve_messages_and_all_transition_states': '2c44e9cb5a661481c823e5957cbfe805e7580c61daf46040783981c7911d7bcf',
    'test_formula_references_are_frozen_before_dual_and_manual_delivery': '3c88791c5ee5f563e89fe16ce0972d413131d99bf3f200368b320dadd737a09d',
    'test_capture_precedes_archive_and_delivery_and_candidates_are_computed_once': 'e5f1dd2ce3fa90b94b7c03e4f85c11ff8248d2f821403abe9c3e2a794ce246fa',
    'test_c1274_path_runs_before_and_independently_of_dual_cvd_failure': '9982d2c99b7d4c6a8401b26e8bee6e9e43b34824170541cda9b92c5b07880040',
    'test_capture_failure_keeps_base_dual_and_successful_precompute': '9d818f2099b64da133d6cc8b66fc5f37937b9ce855c30708f940955fe7669840',
    'test_consumed_input_submission_failure_preserves_delivery': '6d19edde18c2f9ad19aae51ba9b5d5c0e820d1ad7fc7162448f051ebbb3435e2',
    'test_consumed_input_context_setup_failure_preserves_delivery': '4600d95d26f13e8b2e9f6470f67901b4b7c797e81128081d11fe9ce65ecfdc19',
    'test_successfully_empty_precompute_is_observed_and_never_recomputed': '47da3e814aacc4ae8f3e478c41be74754f44ba6d3229aa5a80038a42e300aa25',
    'test_precompute_error_is_deferred_to_original_general_watch_lifecycle': '19f7e5fbbbdea1302f179bbdab323d19422cda6fef8e71e78b1cd790d34739cc',
    'test_general_disabled_capture_has_no_delivery_state_or_error_rethrow': '17936a310e660088d9f307207e1abbdd0ec3da974517f3e4f8ae030d784dc3c3',
}


def pin(node):
    return hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()


def observer_call(node, name=None):
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and
            isinstance(node.func.value, ast.Name) and node.func.value.id == MODULE and
            (name is None or node.func.attr == name))


def named(node, value):
    return isinstance(node, ast.Name) and node.id == value


class EraseOnlyApprovedRuntimeHooks(ast.NodeTransformer):
    def __init__(self):
        self.functions, self.counts = [], {'import': 0, 'initial': 0, 'begin': 0, 'finish': 0, 'cleanup_tail': 0, 'health': 0}
        self.background_hooks = {'import': 0, 'install': 0}
        self.cache_memory_hooks = {'import': 0, 'install': 0, 'health': 0}
        self.stages = []

    def visit_FunctionDef(self, node):
        self.functions.append(node.name)
        result = self.generic_visit(node)
        self.functions.pop()
        return result

    visit_AsyncFunctionDef = visit_FunctionDef

    def in_watch(self):
        assert self.functions and self.functions[0] == 'run_watch_cycle', 'OBSERVER_OUTSIDE_WATCH'

    def visit_Import(self, node):
        if any(alias.name == 'research_memory_investigation' for alias in node.names):
            assert not self.functions and len(node.names) == 1 and node.names[0].asname is None, 'CACHE_MEMORY_IMPORT_SHAPE'
            self.cache_memory_hooks['import'] += 1
            return None
        if any(alias.name == 'research_background_memory' for alias in node.names):
            assert not self.functions and len(node.names) == 1 and node.names[0].asname is None, 'BACKGROUND_IMPORT_SHAPE'
            self.background_hooks['import'] += 1
            return None
        if any(alias.name == MODULE for alias in node.names):
            assert not self.functions and len(node.names) == 1 and node.names[0].asname is None, 'OBSERVER_IMPORT_SHAPE'
            self.counts['import'] += 1
            return None
        return node

    def visit_Assign(self, node):
        if any(named(target, HANDLE) for target in node.targets):
            self.in_watch()
            assert self.functions == ['run_watch_cycle'] and len(node.targets) == 1, 'OBSERVER_HANDLE_SCOPE'
            if isinstance(node.value, ast.Constant) and node.value.value is None:
                self.counts['initial'] += 1
            else:
                assert observer_call(node.value, 'begin') and len(node.value.args) == 1 and named(node.value.args[0], 'watch_scan_id') and not node.value.keywords, 'OBSERVER_BEGIN_SHAPE'
                self.counts['begin'] += 1
            return None
        return self.generic_visit(node)

    def visit_Expr(self, node):
        call = node.value
        if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and
                named(call.func.value, 'research_memory_investigation')):
            assert self.functions == ['main'] and call.func.attr == 'install' and not call.args and not call.keywords, 'CACHE_MEMORY_INSTALL_SHAPE'
            self.cache_memory_hooks['install'] += 1
            return None
        if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and
                named(call.func.value, 'research_background_memory')):
            assert self.functions == ['main'] and call.func.attr == 'install' and not call.keywords and len(call.args) == 1, 'BACKGROUND_INSTALL_SHAPE'
            argument = call.args[0]
            assert isinstance(argument, ast.Call) and named(argument.func, 'globals') and not argument.args and not argument.keywords, 'BACKGROUND_INSTALL_ARGUMENT'
            self.background_hooks['install'] += 1
            return None
        if observer_call(call):
            self.in_watch()
            assert call.args and named(call.args[0], HANDLE), 'OBSERVER_HANDLE_ARGUMENT'
            if call.func.attr == 'stage':
                assert len(call.args) == 2 and isinstance(call.args[1], ast.Constant) and type(call.args[1].value) is str and call.args[1].value in PHASES and not call.keywords, 'OBSERVER_STAGE_SHAPE'
                self.stages.append(call.args[1].value)
            elif call.func.attr == 'cleanup_tail':
                assert self.functions == ['run_watch_cycle'] and len(call.args) == 1 and not call.keywords, 'OBSERVER_CLEANUP_TAIL_SHAPE'
                self.counts['cleanup_tail'] += 1
            else:
                assert call.func.attr == 'finish' and self.functions == ['run_watch_cycle'] and len(call.args) == 1 and len(call.keywords) == 1 and call.keywords[0].arg == 'cycle_status', 'OBSERVER_FINISH_SHAPE'
                status = call.keywords[0].value
                assert (isinstance(status, ast.Call) and isinstance(status.func, ast.Attribute) and named(status.func.value, 'WATCH_RUNTIME') and status.func.attr == 'get' and
                        len(status.args) == 1 and isinstance(status.args[0], ast.Constant) and status.args[0].value == 'last_cycle_status' and not status.keywords), 'OBSERVER_FINISH_STATUS_SHAPE'
                self.counts['finish'] += 1
            return None
        return self.generic_visit(node)

    def visit_Dict(self, node):
        keys, values = [], []
        for key, value in zip(node.keys, node.values):
            if isinstance(key, ast.Constant) and key.value == 'named_cache_memory_investigation':
                assert self.functions == ['health'] and isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute) and named(value.func.value, 'research_memory_investigation') and value.func.attr == 'status' and not value.args and not value.keywords, 'CACHE_MEMORY_HEALTH_SHAPE'
                self.cache_memory_hooks['health'] += 1
                continue
            if isinstance(key, ast.Constant) and key.value == 'runtime_observability':
                assert self.functions == ['health'] and observer_call(value, 'status') and not value.args and not value.keywords, 'OBSERVER_HEALTH_SHAPE'
                self.counts['health'] += 1
            else:
                keys.append(key)
                values.append(value)
        node.keys, node.values = keys, values
        return self.generic_visit(node)

    def visit_Call(self, node):
        assert not observer_call(node), 'UNAPPROVED_OBSERVER_CALL_CONTEXT'
        assert not (isinstance(node.func, ast.Attribute) and named(node.func.value, 'research_memory_investigation')), 'UNAPPROVED_CACHE_MEMORY_CALL_CONTEXT'
        return self.generic_visit(node)


class RuntimeIntegrationPreservationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (ROOT / 'main.py').read_text(encoding='utf-8')
        cls.tree = ast.parse(cls.source)
        cls.functions = {node.name: node for node in cls.tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}

    def test_complete_main_ast_erases_only_the_approved_passive_hooks(self):
        eraser = EraseOnlyApprovedRuntimeHooks()
        original = eraser.visit(deepcopy(self.tree))
        self.assertEqual(eraser.counts, {'import': 1, 'initial': 1, 'begin': 1, 'finish': 1, 'cleanup_tail': 1, 'health': 1})
        self.assertEqual(eraser.background_hooks, {'import': 1, 'install': 1})
        self.assertEqual(eraser.cache_memory_hooks, {'import': 1, 'install': 1, 'health': 1})
        self.assertCountEqual(eraser.stages, PHASES)
        self.assertEqual(len(eraser.stages), len(PHASES))
        self.assertEqual(pin(original), BASELINE_MAIN_AST)

    def test_cache_memory_eraser_rejects_unapproved_shapes(self):
        bad = (
            'import research_memory_investigation as alias',
            'def main():\n research_memory_investigation.install(native())',
            'def wrong():\n research_memory_investigation.install()',
            'async def health():\n return {"named_cache_memory_investigation": research_memory_investigation.status(native())}',
            'async def health():\n return {"named_cache_memory_investigation": native()}',
            'def wrong():\n return research_memory_investigation.status()',
        )
        for source in bad:
            with self.subTest(source=source), self.assertRaises(AssertionError):
                EraseOnlyApprovedRuntimeHooks().visit(ast.parse(source))

    def test_native_formatters_decisions_collection_and_scheduling_source_is_unchanged(self):
        for name, expected in NATIVE_SOURCE.items():
            with self.subTest(function=name):
                text = ast.get_source_segment(self.source, self.functions[name])
                self.assertEqual(hashlib.sha256(text.encode()).hexdigest(), expected)

    def test_begin_and_finally_boundary_preserve_original_cleanup_order(self):
        body = self.functions['run_watch_cycle'].body
        main_try = [node for node in body if isinstance(node, ast.Try)]
        self.assertEqual(len(main_try), 1)
        target = main_try[0]
        self.assertTrue(isinstance(target.body[0], ast.Assign) and observer_call(target.body[0].value, 'begin'))
        self.assertTrue(isinstance(target.finalbody[0], ast.Expr) and observer_call(target.finalbody[0].value, 'finish'))
        self.assertTrue(isinstance(target.finalbody[-1], ast.Expr) and observer_call(target.finalbody[-1].value, 'cleanup_tail'))
        following = target.finalbody[1]
        self.assertTrue(isinstance(following, ast.Expr) and isinstance(following.value, ast.Call) and isinstance(following.value.func, ast.Attribute) and named(following.value.func.value, 'research_alert_decision_watch') and following.value.func.attr == 'finish')
        initial = [node for node in body[:body.index(target)] if isinstance(node, ast.Assign) and any(named(name, HANDLE) for name in node.targets)]
        self.assertEqual(len(initial), 1)
        self.assertIsInstance(initial[0].value, ast.Constant)
        self.assertIsNone(initial[0].value.value)

    def test_existing_watch_fixture_methods_and_all134_assertion_calls_are_preserved(self):
        fixture = ast.parse((ROOT / 'research_watch_decision_capture_watch_selftest.py').read_text(encoding='utf-8'))
        methods = {node.name: pin(node) for node in ast.walk(fixture) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith('test_')}
        self.assertEqual(methods, FIXTURE_METHODS)
        calls = [node for node in ast.walk(fixture) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr.startswith('assert')]
        self.assertEqual(len(calls), 134)
        self.assertEqual(hashlib.sha256(json.dumps([ast.dump(node, include_attributes=False) for node in calls]).encode()).hexdigest(), '3b5c87c9a2755a60ca5c5064856fc9afa3535a835efbc8567777b533661cf397')

    def test_existing_native_capture_assertions_are_not_weakened(self):
        fixture = ast.parse((ROOT / 'research_alert_decision_watch_selftest.py').read_text(encoding='utf-8'))
        calls = [node for node in ast.walk(fixture) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr.startswith('assert')]
        self.assertEqual(len(calls), 43)
        self.assertEqual(hashlib.sha256(json.dumps([ast.dump(node, include_attributes=False) for node in calls]).encode()).hexdigest(), '352d52a1fb3c697f51cb75831ba595ec92817f724f2e80083df27191f65d65d8')

    def test_eraser_rejects_native_side_effects_hidden_in_diagnostic_arguments(self):
        eraser = EraseOnlyApprovedRuntimeHooks()
        eraser.functions = ['run_watch_cycle']
        node = ast.parse("research_watch_runtime.stage(runtime_observation_handle, native_side_effect())").body[0]
        with self.assertRaisesRegex(AssertionError, 'OBSERVER_STAGE_SHAPE'):
            eraser.visit(node)

    def test_cleanup_tail_cannot_hide_native_work_or_keyword_arguments(self):
        for text in ("research_watch_runtime.cleanup_tail(runtime_observation_handle, native_side_effect())",
                     "research_watch_runtime.cleanup_tail(runtime_observation_handle, extra=native_side_effect())"):
            eraser = EraseOnlyApprovedRuntimeHooks()
            eraser.functions = ['run_watch_cycle']
            with self.subTest(source=text), self.assertRaisesRegex(AssertionError, 'OBSERVER_CLEANUP_TAIL_SHAPE'):
                eraser.visit(ast.parse(text).body[0])

    def test_eraser_rejects_observer_hooks_outside_watch_and_native_wrapping(self):
        eraser = EraseOnlyApprovedRuntimeHooks()
        node = ast.parse("research_watch_runtime.stage(runtime_observation_handle, 'INPUTS_READY')").body[0]
        with self.assertRaisesRegex(AssertionError, 'OBSERVER_OUTSIDE_WATCH'):
            eraser.visit(node)
        node = ast.parse("research_watch_runtime.wrap(native_side_effect())", mode='eval').body
        with self.assertRaisesRegex(AssertionError, 'UNAPPROVED_OBSERVER_CALL_CONTEXT'):
            EraseOnlyApprovedRuntimeHooks().visit(node)


if __name__ == '__main__':
    unittest.main(verbosity=2)
