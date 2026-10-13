"""Research simulator acceptance fixtures; never added to the live suite."""
import io,json,tempfile,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
import numpy as np
import pandas as pd
import data
from data import DAY,MS,SCALE
from engine import MarketData,exit_path,funding_cost,make_signals,periods,references,simulate

def ts(x):return pd.Timestamp(x,tz='UTC').value//10**6

def fixture():
    times=np.arange(ts('2022-11-07'),ts('2022-11-10'),MS)
    df=pd.DataFrame({'time':times,'open':95*SCALE,'high':95*SCALE+SCALE//100,'low':95*SCALE-SCALE//100,
                     'close':95*SCALE,'quote_volume':100.,'volume':1.,'trades':1})
    def bar(i,o,h,l,c):df.loc[i,['open','high','low','close']]=np.array([o,h,l,c])*SCALE
    i=1441
    bar(i,99,101,98,99);bar(1444,100,102,99,101);bar(1449,100,100,98,99);bar(1450,99,100,98,99)
    daily=pd.DataFrame({'time':np.arange(ts('2022-07-01'),ts('2022-11-10'),DAY),'high':100*SCALE,'low':90*SCALE})
    members=pd.DataFrame({'day':[ts('2022-11-08'),ts('2022-11-09')],'rank':[1,1]})
    return df,daily,members

def market(df=None):
    base,daily,members=fixture()
    return MarketData('TESTUSDT',base if df is None else df,daily,members,pd.DataFrame())

def selected(events,kind='live_1m'):
    return [e for e in events if e['level']=='monday_high' and e['side']=='short' and e['sweep_type']==kind and e['timeframe']==5 and e['sweep_time']==ts('2022-11-08 00:01')]

class ReferencesTests(unittest.TestCase):
    def test_monday_and_quarter_boundaries(self):
        self.assertIsNone(periods(ts('2023-01-02'))['monday'])
        self.assertEqual(periods(ts('2023-01-03'))['monday'],(ts('2023-01-02'),ts('2023-01-03')))
        self.assertEqual(periods(ts('2023-01-01'))['previous_quarter'],(ts('2022-10-01'),ts('2023-01-01')))
        self.assertEqual(periods(ts('2023-01-01'))['previous_week'],(ts('2022-12-19'),ts('2022-12-26')))
    def test_missing_daily_reference_unavailable(self):
        _,daily,_=fixture();daily=daily[daily.time!=ts('2022-10-15')]
        r=references(daily,[ts('2022-11-08')])[ts('2022-11-08')]
        self.assertNotIn('previous_month_high',r);self.assertIn('monday_high',r)

class SignalsTests(unittest.TestCase):
    def test_consecutive_confirmation_stop_and_entry(self):
        m=market();events,_=make_signals(m,['train']);e=selected(events)[0]
        self.assertEqual(e['status'],'confirmed');self.assertEqual(e['confirmation_time'],ts('2022-11-08 00:10'))
        self.assertEqual(e['baseline_stop_units'],102*SCALE)
        trades,_=simulate(m,[e]);t=next(t for t in trades if t['stop_rule']=='baseline' and t['target_rule']=='2R' and t['entry_rule']=='next_open')
        self.assertEqual(t['entry'],99);self.assertEqual(t['stop'],102);self.assertEqual(t['target'],93)
        self.assertEqual(t['entry_time'],ts('2022-11-08 00:10'))
        self.assertGreater(t['fee_base_r'],0);self.assertLess(t['net_base_no_funding_r'],t['gross_r'])
    def test_equality_does_not_reclaim_and_no_sliding_first_bar(self):
        df,_,_=fixture();df.loc[1449,['close','high']]=[100*SCALE,101*SCALE]
        e=selected(make_signals(market(df),['train'])[0])[0]
        self.assertEqual(e['status'],'next_confirmation_failed')
    def test_missing_minute_invalidates_confirmation(self):
        df,_,_=fixture();df=df.drop(index=1447)
        e=selected(make_signals(market(df),['train'])[0])[0]
        self.assertNotEqual(e['status'],'confirmed')
    def test_long_is_mirror(self):
        df,daily,members=fixture()
        old=df.copy()
        for a,b in [('open','open'),('high','low'),('low','high'),('close','close')]:df[a]=190*SCALE-old[b]
        m=MarketData('TESTUSDT',df,daily,members,pd.DataFrame())
        events,_=make_signals(m,['train'])
        e=next(e for e in events if e['level']=='monday_low' and e['side']=='long' and e['sweep_type']=='live_1m' and e['timeframe']==5 and e['sweep_time']==ts('2022-11-08 00:01'))
        self.assertEqual(e['status'],'confirmed');self.assertEqual(e['baseline_stop_units'],88*SCALE)
    def test_prior_features_and_ticks_do_not_use_future(self):
        m=market();df,_,_=fixture();df.loc[df.index>=1442,'quote_volume']=1e9
        n=market(df);i=1441
        self.assertEqual(m.features(i,-1,100*SCALE),n.features(i,-1,100*SCALE))
        np.testing.assert_array_equal(m.tick[:2880],n.tick[:2880])
    def test_controls_and_all_three_sweeps_exist(self):
        events,_=make_signals(market(),['train'])
        self.assertEqual({e['sweep_type'] for e in events},{'live_1m','delayed_reclaim','close_beyond','reclaim_only'})
    def test_signal_close_has_no_signal_candle_exposure(self):
        m=market();e=selected(make_signals(m,['train'])[0])[0]
        trades,_=simulate(m,[e])
        self.assertTrue(all(t['exit_time']>=t['entry_time']>=e['confirmation_time'] for t in trades))
    def test_pending_setup_expires_at_six_hours(self):
        df,daily,members=fixture();df.loc[1444,['open','high','low','close']]=[95*SCALE,96*SCALE,94*SCALE,95*SCALE]
        m=MarketData('TESTUSDT',df,daily,members,pd.DataFrame())
        e=selected(make_signals(m,['train'])[0])[0]
        self.assertEqual(e['status'],'confirmation_timeout')

class ExitsTests(unittest.TestCase):
    def fake(self,o,h,l,c,valid=None):
        n=len(o);return SimpleNamespace(o=np.array(o),h=np.array(h),l=np.array(l),c=np.array(c),t=np.arange(n)*MS,
                                        n=n,gaps=np.flatnonzero(~np.array(valid if valid is not None else [True]*n)))
    def test_same_bar_stop_first(self):
        m=self.fake([100],[112],[79],[100]);r=exit_path(m,0,100,110,80,-1,10*MS)
        self.assertEqual(r['gross_r'],-1);self.assertTrue(r['ambiguous_stop_first'])
    def test_gap_open_stop_slips_and_target_capped(self):
        a=exit_path(self.fake([112],[114],[75],[100]),0,100,110,80,-1,10*MS)
        b=exit_path(self.fake([78],[114],[75],[100]),0,100,110,80,-1,10*MS)
        self.assertEqual(a['gross_r'],-1.2);self.assertEqual(b['gross_r'],2)
        self.assertFalse(a['ambiguous_stop_first']);self.assertFalse(b['ambiguous_stop_first'])
    def test_missing_minute_exits_before_gap(self):
        m=self.fake([100,100,0],[101,102,0],[99,99,0],[100,101,0],[True,True,False])
        r=exit_path(m,0,100,110,80,-1,10*MS)
        self.assertEqual(r['exit_reason'],'data_gap');self.assertEqual(r['exit_time'],2*MS)
    def test_max_holding_and_split_exit(self):
        m=self.fake([100]*4,[101]*4,[99]*4,[100]*4)
        self.assertEqual(exit_path(m,0,100,110,80,-1,10*MS,hold_minutes=2)['exit_reason'],'max_hold')
        self.assertEqual(exit_path(m,0,100,110,80,-1,2*MS)['exit_reason'],'split_end')
    def test_funding_sign_and_timing(self):
        m=SimpleNamespace(t=np.array([1]),ft=np.array([0,2,4]),fc=np.array([0.,0.,2.,2.]),fb=np.zeros(4),fg=np.zeros(4))
        self.assertEqual(funding_cost(m,0,3,1,10)[0],.2)
        self.assertEqual(funding_cost(m,0,3,-1,10)[0],-.2)
        self.assertEqual(funding_cost(m,0,1,1,10)[0],0)
    def test_funding_jitter_uses_only_completed_price_candle(self):
        df,daily,members=fixture()
        df.loc[1440,['high','close']]=999*SCALE
        f=pd.DataFrame({'time':[ts('2022-11-07 16:00'),ts('2022-11-08')+19,ts('2022-11-08 08:00')+10],
                        'hours':[8,8,8],'rate':[0.01,0.01,0.01]})
        m=MarketData('TESTUSDT',df,daily,members,f)
        amount,status=funding_cost(m,1439,ts('2022-11-08 00:05'),1,10*SCALE)
        self.assertAlmostEqual(amount,.095)
        self.assertEqual(status,'verified_rates_trade_close_notional_proxy')

class DataTests(unittest.TestCase):
    def test_duplicates_malformed_boundaries_and_gaps_not_filled(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp);z=path/'TESTUSDT-1m-2022-11.zip';a=ts('2022-11-01')
            rows=[]
            for minute in [0,1,1,3,4]:
                t=a+minute*MS
                rows.append([t,100,101,99,100,1,t+MS-1,100,1,0,0,0])
            rows[-1][6]+=1
            with zipfile.ZipFile(z,'w') as f:f.writestr('test.csv',pd.DataFrame(rows).to_csv(index=False,header=False))
            meta={'status':'verified','local_archive':str(z)}
            with patch.object(data,'fetch',return_value=meta),patch.object(data,'PARSED',path),patch.object(data,'RAW',path):
                df=data.parse('TESTUSDT','1m','2022-11')
            self.assertEqual(list(df.time),[a,a+3*MS]);self.assertEqual(meta['duplicate_rows'],2)
            self.assertEqual(meta['malformed_rows'],1);self.assertEqual(meta['internal_gap_intervals'],2)
    def test_bad_checksum_never_admitted(self):
        with tempfile.TemporaryDirectory() as tmp:
            responses=[SimpleNamespace(status_code=200,text='0'*64+'  test.zip'),SimpleNamespace(status_code=200,content=b'wrong',headers={})]
            with patch.object(data,'RAW',Path(tmp)),patch.object(data,'get',side_effect=responses):m=data.fetch('TESTUSDT','1m','2022-11')
            self.assertEqual(m['status'],'unverified');self.assertFalse(Path(m['local_archive']).exists())

if __name__=='__main__':unittest.main()
