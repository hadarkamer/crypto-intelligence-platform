"""Changing a setting requires a fresh read; an already selected one requires only the gate."""
import unittest
from unittest.mock import Mock, patch

from model1_execution import StageFailure
import modern_map_reader as reader


class VerifiedSelection(unittest.TestCase):
    def test_correct_setting_read_once_and_handles_released(self):
        control = Mock()
        control.inner_text.return_value = 'Viridis Default'
        with patch.object(reader, 'option_control', return_value=control) as resolve, patch.object(reader, 'gate') as gate:
            reader.select_option(object(), 'Colours', 'Viridis')
        resolve.assert_called_once()
        control.inner_text.assert_called_once()
        control.click.assert_not_called()
        control.dispose.assert_called_once()
        gate.assert_called_once()

    def test_replaced_combobox_checked_after_click(self):
        page, first, second, option = (Mock() for _ in range(4))
        first.inner_text.return_value = 'Price'
        second.inner_text.return_value = 'Heat'
        with patch.object(reader, 'option_control', side_effect=[first, second]) as resolve, \
             patch.object(reader, 'visible', return_value=[page]), patch.object(reader, 'one', return_value=option), \
             patch.object(reader, 'gate') as gate:
            reader.select_option(page, 'Lens', 'Heat')
        self.assertEqual(resolve.call_count, 2)
        first.click.assert_called_once()
        option.click.assert_called_once()
        first.dispose.assert_called_once()
        second.dispose.assert_called_once()
        gate.assert_called_once()

    def test_correct_setting_and_choice_still_check_gate(self):
        control = Mock()
        control.inner_text.return_value = 'Heat'
        page = object()
        with patch.object(reader, 'option_control', return_value=control), \
             patch.object(reader, 'choice', return_value=True) as choice, \
             patch.object(reader, 'gate', side_effect=StageFailure('source_login_required')) as gate:
            with self.assertRaises(StageFailure):
                reader.select_option(page, 'Lens', 'Heat')
            with self.assertRaises(StageFailure):
                reader.select_choice(page, 'Coin')
            choice.assert_called_once()
            self.assertEqual(gate.call_count, 2)
        control.dispose.assert_called_once()

    def test_default_gate_does_not_block_model_change_before_requested_range(self):
        page, target = Mock(), Mock()
        with patch.object(reader, 'choice', side_effect=[False, True]) as choice, \
             patch.object(reader, 'choice_target', return_value=target), patch.object(reader, 'gate') as gate:
            reader.select_choice(page, 'Model 3', requested=False)
        self.assertEqual(choice.call_count, 2)
        target.click.assert_called_once()
        gate.assert_not_called()
