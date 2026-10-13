import unittest
import numpy as np
import pandas as pd
from screen import CUTOFF, CORE, cutoff_masks, join_screen, matched_bootstrap


class SettlementScreenTests(unittest.TestCase):
    def frame(self):
        return pd.DataFrame({'trade_id':['before','exact_exit','crossing','exact_entry','late','other'],
            'symbol':['SRMUSDT']*5+['BTCUSDT'],
            'entry_time':[CUTOFF-2,CUTOFF-2,CUTOFF-2,CUTOFF,CUTOFF+1,CUTOFF+1],
            'exit_time':[CUTOFF-1,CUTOFF,CUTOFF+1,CUTOFF,CUTOFF+2,CUTOFF+2],
            'unchanged_value':[1.,2.,3.,4.,5.,6.]})

    def flags(self):
        return pd.DataFrame({'trade_id':['crossing','exact_entry','late'],'settlement_time':[CUTOFF]*3,'reason':['crosses','entry','entry']})

    def test_exact_cutoff_rules(self):
        entry,crossing=cutoff_masks(self.frame())
        self.assertEqual(entry.tolist(),[False,False,False,True,True,False])
        self.assertEqual(crossing.tolist(),[False,False,True,False,False,False])

    def test_partition_join_preserves_values_and_reconciles(self):
        frame=self.frame();keep,q=join_screen(frame,self.flags())
        self.assertEqual(set(keep.trade_id),{'before','exact_exit','other'})
        self.assertEqual(set(q.trade_id),{'crossing','exact_entry','late'})
        restored=pd.concat([keep,q[frame.columns]]).sort_values('trade_id').reset_index(drop=True)
        pd.testing.assert_frame_equal(restored,frame.sort_values('trade_id').reset_index(drop=True))

    def test_missing_known_flag_blocks_analysis(self):
        with self.assertRaisesRegex(AssertionError,'disagree'):join_screen(self.frame(),self.flags().iloc[:2])

    def test_wrongly_flagged_eligible_row_blocks_analysis(self):
        flags=self.flags();flags.loc[3]=['before',CUTOFF,'wrong']
        with self.assertRaisesRegex(AssertionError,'disagree'):join_screen(self.frame(),flags)

    def test_duplicate_flags_block_analysis(self):
        flags=pd.concat([self.flags(),self.flags().iloc[:1]])
        with self.assertRaisesRegex(AssertionError,'Duplicate'):join_screen(self.frame(),flags)

    def test_interval_rng_does_not_shift_unaffected_groups(self):
        rows=[]
        for stop in ['baseline','excursion']:
            for symbol in ['A','B','C']:
                for week in range(4):
                    rows.append(dict(zip(CORE,['train','reclaim_only',1,stop,'opposing','next_open']),
                        symbol=symbol,entry_time=CUTOFF+week*7*86400000,
                        net_base_no_funding_r=float(week)+ord(symbol)/100))
        before=pd.DataFrame(rows)
        after=before[~((before.stop_rule=='baseline')&(before.symbol=='C'))]
        screened,baseline=matched_bootstrap(after,before)
        pd.testing.assert_frame_equal(screened[screened.stop_rule=='excursion'].reset_index(drop=True),baseline[baseline.stop_rule=='excursion'].reset_index(drop=True))
        self.assertEqual(screened[screened.stop_rule=='baseline'].iloc[0].symbols,2)
        self.assertEqual(baseline[baseline.stop_rule=='baseline'].iloc[0].symbols,3)


if __name__=='__main__':unittest.main()
