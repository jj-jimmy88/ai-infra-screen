#!/usr/bin/env python3
"""AI-Infra 10-Factor Screen - weekly data refresh.
Recomputes all 10 measured factors + financials for the universe, writes data.json.
Run:  python update_data.py            (fresh fetch, ~4 min)
      python update_data.py --cache    (reuse cached fetch files if present)
"""
import json, sys, math, datetime as dt, warnings, os
warnings.filterwarnings('ignore')
import yfinance as yf, pandas as pd, numpy as np
from concurrent.futures import ThreadPoolExecutor

UNIVERSE=[("MNTS","Momentus"),("RDW","Redwire"),("AMPG","AmpliTech Group"),("UMAC","Unusual Machines"),
("TE","T1 Energy"),("RKLB","Rocket Lab"),("ALAB","Astera Labs"),("MU","Micron"),
("SPCE","Virgin Galactic"),("LUNR","Intuitive Machines"),("ASTS","AST SpaceMobile"),
("000660.KS","SK hynix"),("SMCI","Super Micro Computer"),("BB","BlackBerry"),("ARM","Arm Holdings"),
("SIDU","Sidus Space"),("NBIS","Nebius Group"),("HLIT","Harmonic"),("SNDK","Sandisk"),
("POET","POET Technologies"),("BKSY","BlackSky"),("QCOM","Qualcomm"),("SATL","Satellogic"),
("QBTS","D-Wave Quantum"),("AMD","AMD"),("RGTI","Rigetti Computing"),("IONQ","IonQ"),
("APLD","Applied Digital"),("FLY","Firefly Aerospace"),("OCC","Optical Cable"),("STX","Seagate"),
("CRWD","CrowdStrike"),("SPIR","Spire Global"),("PL","Planet Labs"),("IREN","IREN Limited"),
("PANW","Palo Alto Networks"),("INTC","Intel"),("MRCY","Mercury Systems"),("STM","STMicroelectronics"),
("CORZ","Core Scientific"),("IRDM","Iridium"),("GFS","GlobalFoundries"),("WDC","Western Digital"),
("CRDO","Credo Technology"),("MRVL","Marvell"),("HPE","Hewlett Packard Ent"),("BE","Bloom Energy"),
("EOSE","Eos Energy Enterprises"),("ONDS","Ondas Holdings"),("TSAT","Telesat"),("RCAT","Red Cat Holdings"),
("LRCX","Lam Research"),("HPQ","HP Inc"),("COHR","Coherent"),("AAOI","Applied Optoelectronics"),
("NNE","Nano Nuclear Energy"),("GLW","Corning"),("CIEN","Ciena"),("NSU.V","North Shore Uranium"),
("PRLB","Proto Labs"),("AMAT","Applied Materials"),("ASML","ASML"),("VIAV","Viavi Solutions"),
("IBM","IBM"),("AVAV","AeroVironment"),("FCX","Freeport-McMoRan"),("ATI","ATI Inc"),
("MP","MP Materials"),("LITE","Lumentum"),("TMQ","Trilogy Metals"),("SOLS","Solstice Advanced Materials"),
("TSM","TSMC"),("AVGO","Broadcom"),("KTOS","Kratos Defense"),("IPGP","IPG Photonics"),
("KLAC","KLA Corp"),("LAC","Lithium Americas"),("FN","Fabrinet"),("SMR","NuScale Power"),
("SATS","EchoStar"),("LMT","Lockheed Martin"),("PUSA","Aureus Greenway / Powerus"),("VRT","Vertiv"),
("RTX","RTX"),("APH","Amphenol"),("CRWV","CoreWeave"),("CRML","Critical Metals"),
("PLTR","Palantir"),("NVDA","NVIDIA"),("MSFT","Microsoft"),("BA","Boeing"),("OKLO","Oklo"),
("NNDM","Nano Dimension"),("HXL","Hexcel"),("LHX","L3Harris"),("NOC","Northrop Grumman"),
("ENR.DE","Siemens Energy"),("CCJ","Cameco"),("UEC","Uranium Energy"),("CEG","Constellation Energy"),
("URA","Global X Uranium ETF"),("ANET","Arista Networks"),("AMTM","Amentum"),("GEV","GE Vernova"),
("DNN","Denison Mines"),("UUUU","Energy Fuels"),("LEU","Centrus Energy"),
("SIVE.ST","Sivers Semiconductors"),("TSEM","Tower Semiconductor"),("VST","Vistra"),
("TLN","Talen Energy"),("BWXT","BWX Technologies"),("SNPS","Synopsys"),("NOK","Nokia"),
("MOG-A","Moog"),("USAR","USA Rare Earth"),
("FTNT","Fortinet"),("ZS","Zscaler"),("S","SentinelOne"),("OKTA","Okta"),("CYBR","CyberArk"),
("NET","Cloudflare"),("RBRK","Rubrik"),("ORCL","Oracle"),
("6324.T","Harmonic Drive"),("6268.T","Nabtesco"),("SHA.DE","Schaeffler"),("2050.HK","Sanhua Intelligent"),
("NJDCY","Nidec"),("ALGM","Allegro MicroSystems"),("MELE.BR","Melexis"),
("DRAM","Roundhill Memory ETF")]
ETF={"DRAM","URA"}
# Thematic sector labels for the Sector column (falls back to Yahoo industry/sector if unmapped)
SECTOR={
 'PANW':'Cybersecurity','CRWD':'Cybersecurity','FTNT':'Cybersecurity','ZS':'Cybersecurity','S':'Cybersecurity',
 'OKTA':'Cybersecurity','CYBR':'Cybersecurity','NET':'Cybersecurity','RBRK':'Cybersecurity','BB':'Cybersecurity',
 'PLTR':'AI Software','MSFT':'AI Software','IBM':'AI Software','ORCL':'AI Software','NBIS':'AI Software',
 'APLD':'AI Datacenter','IREN':'AI Datacenter','CORZ':'AI Datacenter','CRWV':'AI Datacenter',
 'NVDA':'AI Compute','AMD':'AI Compute','INTC':'AI Compute','AVGO':'AI Compute','QCOM':'AI Compute',
 'ARM':'AI Compute','ALAB':'AI Compute','CRDO':'AI Compute','MRVL':'AI Compute','ANET':'AI Compute',
 'HPE':'AI Compute','HPQ':'Hardware','STM':'Semiconductors',
 'MU':'Memory/Storage','SNDK':'Memory/Storage','WDC':'Memory/Storage','STX':'Memory/Storage',
 '000660.KS':'Memory/Storage','DRAM':'Memory/Storage','SMCI':'AI Compute',
 'TSM':'Chip Foundry','GFS':'Chip Foundry','TSEM':'Chip Foundry',
 'AMAT':'Chip Equipment','ASML':'Chip Equipment','LRCX':'Chip Equipment','KLAC':'Chip Equipment','SNPS':'Chip EDA',
 'COHR':'Optical/Networking','LITE':'Optical/Networking','VIAV':'Optical/Networking','CIEN':'Optical/Networking',
 'AAOI':'Optical/Networking','FN':'Optical/Networking','IPGP':'Optical/Networking','GLW':'Optical/Networking',
 'HLIT':'Optical/Networking','NOK':'Optical/Networking','POET':'Optical/Networking','SIVE.ST':'Optical/Networking',
 'OCC':'Optical/Networking','AMPG':'Components','APH':'Components',
 '6324.T':'Robotics','6268.T':'Robotics','SHA.DE':'Robotics','2050.HK':'Robotics','NJDCY':'Robotics',
 'MOG-A':'Robotics','ALGM':'Robotics','MELE.BR':'Robotics',
 'RKLB':'Space/Satellites','ASTS':'Space/Satellites','LUNR':'Space/Satellites','BKSY':'Space/Satellites',
 'SATL':'Space/Satellites','SPIR':'Space/Satellites','PL':'Space/Satellites','IRDM':'Space/Satellites',
 'SATS':'Space/Satellites','TSAT':'Space/Satellites','RDW':'Space/Satellites','SIDU':'Space/Satellites',
 'FLY':'Space/Satellites','MNTS':'Space/Satellites','SPCE':'Space/Satellites',
 'UMAC':'Drones','RCAT':'Drones','ONDS':'Drones','AVAV':'Defense',
 'LMT':'Defense','RTX':'Defense','NOC':'Defense','LHX':'Defense','KTOS':'Defense','MRCY':'Defense',
 'AMTM':'Defense','BWXT':'Nuclear','BA':'Aerospace','HXL':'Aerospace',
 'MP':'Rare Earths','USAR':'Rare Earths','LAC':'Critical Materials','TMQ':'Critical Materials',
 'CRML':'Critical Materials','SOLS':'Critical Materials','NSU.V':'Nuclear','FCX':'Materials',
 'PUSA':'Critical Materials','ATI':'Adv Materials',
 'OKLO':'Nuclear','SMR':'Nuclear','NNE':'Nuclear','CCJ':'Nuclear','UEC':'Nuclear','DNN':'Nuclear',
 'UUUU':'Nuclear','LEU':'Nuclear','URA':'Nuclear',
 'CEG':'Power','VST':'Power','TLN':'Power','GEV':'Power','ENR.DE':'Power','VRT':'Power','BE':'Power','EOSE':'Power',
 'IONQ':'Quantum','RGTI':'Quantum','QBTS':'Quantum',
 'NNDM':'Adv Manufacturing','PRLB':'Adv Manufacturing','TE':'Solar',
}
# MANUAL SECTION - edit when situations change, then re-run:
VETO={"SMCI":"DOJ export-fraud indictment (Mar-2026) + securities class actions",
      "CRWV":"Active securities-fraud class action (demand/delay misrepresentation)"}
WT={'F1':3,'F2':2.5,'F3':3,'F4':2.5,'F5':2,'F6':2,'F7':2.5,'F8':2,'F9':1.5,'F10':1.5}
MX={'F1':3,'F2':2,'F3':3,'F4':3,'F5':2,'F6':2,'F7':2,'F8':2,'F9':2,'F10':2}
FULLWT=round(sum(WT.values()),1)  # 22.5
TW={'F1':'Analysts raising estimates','F2':'Strong earnings beats','F3':'Steady price uptrend',
    'F4':'Fast revenue growth','F5':'Expanding margins','F6':'High expected growth',
    'F7':'Attractive valuation','F8':'Solid balance sheet','F9':'Analysts agree on value','F10':'Insiders buying'}
HW={'F1':'Estimates being cut','F2':'Earnings misses','F3':'Weak / erratic price',
    'F4':'Slow revenue growth','F5':'Margins compressing','F6':'Low expected growth',
    'F7':'Stretched valuation','F8':'Dilution / debt risk','F9':'Analysts divided','F10':'Insider selling'}
CACHE='--cache' in sys.argv
def jload(p): return json.load(open(p)) if os.path.exists(p) else None

# --- user-added tickers (change #11): any symbols listed in extra_tickers.txt
# (one per line, '#' comments allowed) are appended to the universe and scored
# by the exact same engine on the next refresh. ---
def load_extra():
    p=os.path.join(os.path.dirname(os.path.abspath(__file__)),'extra_tickers.txt')
    if not os.path.exists(p): return []
    have={t for t,_ in UNIVERSE}; out=[]
    for line in open(p):
        s=line.split('#',1)[0].strip().upper()
        if s and s not in have: have.add(s); out.append((s,s))
    return out
UNIVERSE=UNIVERSE+load_extra()

def fetch_prices():
    if CACHE and jload('prices.json'): return jload('prices.json')
    tks=[t for t,_ in UNIVERSE]
    data=yf.download(tks,period="13mo",interval="1d",group_by="ticker",auto_adjust=True,progress=False,threads=True)
    today=dt.date.today(); cut26=today-dt.timedelta(days=182); out={}
    for t in tks:
        rec={}
        try:
            s=data[t]['Close'].dropna()
            if len(s)<30: out[t]={}; continue
            cur=float(s.iloc[-1]); rec['price']=round(cur,2)
            s26=s[s.index.date>=cut26]; past=float(s26.iloc[0]) if len(s26)>5 else float(s.iloc[0])
            rec['ret26']=round((cur/past-1)*100,1)
            r=s.pct_change().dropna().iloc[-126:]
            rec['vol']=round(float(r.std()*np.sqrt(252))*100,1)
            ma=float(s.rolling(200).mean().iloc[-1]) if len(s)>=200 else float(s.mean())
            rec['pct200']=round((cur/ma-1)*100,1)
        except Exception: pass
        out[t]=rec
    json.dump(out,open('prices.json','w')); return out

def fetch_fund():
    if CACHE and jload('fund1.json'): return jload('fund1.json')
    def grab(t):
        rec={}; tk=yf.Ticker(t)
        try:
            info=tk.info
            for a,b in [('forwardPE','fpe'),('marketCap','mcap'),('totalCash','cash'),('totalDebt','debt'),('numberOfAnalystOpinions','nA')]:
                rec[b]=info.get(a)
            rec['fcur']=info.get('financialCurrency') or 'USD'
            rec['industry']=info.get('industry'); rec['ysector']=info.get('sector')
        except Exception: pass
        try:
            tr=tk.eps_trend
            def g(p):
                try:
                    r=tr.loc[p]; c,o=r.get('current'),r.get('90daysAgo')
                    return [None if c is None or pd.isna(c) else float(c),None if o is None or pd.isna(o) else float(o)]
                except Exception: return [None,None]
            rec['e0y']=g('0y'); rec['e1y']=g('+1y')
        except Exception: rec['e0y']=[None,None]; rec['e1y']=[None,None]
        try:
            rv=tk.eps_revisions; u=d=0
            for p in ['0y','+1y']:
                try:
                    r=rv.loc[p]; u+=int(r.get('upLast30days') or 0); d+=int(r.get('downLast30days') or 0)
                except Exception: pass
            rec['rev']=[u,d]
        except Exception: rec['rev']=[0,0]
        try:
            q=tk.quarterly_income_stmt
            if q is not None and 'Total Revenue' in q.index:
                cols=list(q.columns)
                revq=[float(q.loc['Total Revenue',c]) for c in cols if not pd.isna(q.loc['Total Revenue',c])]
                rec['revq']=revq[:8]
                if 'Gross Profit' in q.index:
                    gpq=[float(q.loc['Gross Profit',c]) for c in cols if not pd.isna(q.loc['Gross Profit',c])]
                    n=min(len(revq),len(gpq))
                    if n>=5:
                        k=min(4,n-4)
                        rec['gm_delta_bps']=round((sum(gpq[:4])/sum(revq[:4])-sum(gpq[4:4+k])/sum(revq[4:4+k]))*10000)
                if 'Net Income' in q.index:
                    niq=[]; nidt=[]
                    for c in cols:
                        v=q.loc['Net Income',c]
                        if not pd.isna(v): niq.append(float(v)); nidt.append(str(c)[:7])
                    rec['niq']=niq[:6]; rec['nidt']=nidt[:6]
        except Exception: pass
        try:
            a=tk.income_stmt
            if a is not None:
                cols=list(a.columns)
                if 'Total Revenue' in a.index:
                    for c in cols:
                        v=a.loc['Total Revenue',c]
                        if not pd.isna(v): rec['fy_rev']=float(v); rec['fy_year']=str(c)[:4]; break
                if 'Net Income' in a.index:
                    for c in cols:
                        v=a.loc['Net Income',c]
                        if not pd.isna(v): rec['fy_profit']=float(v); break
        except Exception: pass
        try:
            b=tk.quarterly_balance_sheet
            if b is not None and 'Ordinary Shares Number' in b.index:
                sh=[float(x) for x in b.loc['Ordinary Shares Number'].values[:5] if not pd.isna(x)]
                if len(sh)>=4: rec['dil_yoy']=round((sh[0]/sh[min(4,len(sh)-1)]-1)*100,1)
        except Exception: pass
        try:
            ip=tk.insider_purchases
            if ip is not None and len(ip)>0:
                row=ip[ip.iloc[:,0].astype(str).str.contains('% Net Shares Purchased',na=False)]
                if len(row)>0:
                    v=row.iloc[0,1]
                    if v is not None and not pd.isna(v): rec['insider_net_pct']=round(float(v),2)
        except Exception: pass
        return t,rec
    out={}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for t,rec in ex.map(grab,[t for t,_ in UNIVERSE]): out[t]=rec
    json.dump(out,open('fund1.json','w')); return out

def fetch_precise():
    if CACHE and jload('prec1.json'): return jload('prec1.json')
    def grab(t):
        rec={}; tk=yf.Ticker(t)
        try:
            eh=tk.earnings_history
            if eh is not None and len(eh)>0:
                sp=[]
                for x in eh['surprisePercent'].dropna().values[-2:]:
                    x=float(x); sp.append(x*100 if abs(x)<=1.5 else x)
                if sp: rec['surp']=round(sum(sp)/len(sp),1)
        except Exception: pass
        try:
            re_=tk.revenue_estimate; g=[]
            for p in ['0y','+1y']:
                try:
                    v=re_.loc[p].get('growth')
                    if v is not None and not pd.isna(v): g.append(float(v)*100)
                except Exception: pass
            if g: rec['fwdrevg']=round(sum(g)/len(g),1)
        except Exception: pass
        try:
            pt=tk.analyst_price_targets
            hi,lo,me=pt.get('high'),pt.get('low'),pt.get('mean')
            if all(v is not None for v in (hi,lo,me)) and me>0: rec['disp']=round((hi-lo)/me*100,1)
        except Exception: pass
        return t,rec
    out={}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for t,rec in ex.map(grab,[t for t,_ in UNIVERSE]): out[t]=rec
    json.dump(out,open('prec1.json','w')); return out

def fetch_news():
    import urllib.request
    def grab(t):
        sym=t  # all tickers are already in Yahoo's native symbol format (e.g. MOG-A, 6324.T, SHA.DE)
        url=f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={sym}&region=US&lang=en-US"
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0'})
            raw=urllib.request.urlopen(req,timeout=8).read().decode('utf-8',errors='ignore')
            import re as _re
            items=_re.findall(r'<item>(.*?)</item>',raw,_re.S)[:4]
            out=[]
            for it in items:
                ti=_re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>',it,_re.S)
                li=_re.search(r'<link>(.*?)</link>',it,_re.S)
                da=_re.search(r'<pubDate>(.*?)</pubDate>',it,_re.S)
                if ti: out.append({'t':ti.group(1).strip()[:140],'u':(li.group(1).strip() if li else ''),'d':(da.group(1).strip()[:16] if da else '')})
            return t,out
        except Exception: return t,[]
    out={}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for t,items in ex.map(grab,[t for t,_ in UNIVERSE]): out[t]=items
    return out

def pctchg(cur,old):
    if cur is None or old is None or old==0: return None
    return (cur-old)/abs(old)*100
def qoq(a,b):
    if a is None or b is None or b==0: return None
    return round((a-b)/abs(b)*100,1)

def score(P,F,X):
    rows=[]
    for t,name in UNIVERSE:
        p,f,x=P.get(t,{}),F.get(t,{}),X.get(t,{})
        r={'t':t,'name':name,'price':p.get('price'),'ret26':p.get('ret26'),'vol':p.get('vol'),
           'pct200':p.get('pct200'),'mcap':f.get('mcap'),'fpe':f.get('fpe')}
        isetf=t in ETF
        ch=[c for c in (pctchg(*(f.get(k,[None,None]))) for k in ('e0y','e1y')) if c is not None]
        rev90=round(sum(ch)/len(ch),1) if ch else None
        u,d=f.get('rev',[0,0])
        if isetf or rev90 is None: f1=None
        else:
            f1=3 if rev90>=10 else (2 if rev90>=3 else (1 if rev90>=0 else 0))
            if f1==1 and (u-d)>=3: f1=2
            if f1==0 and (u-d)>=5: f1=1
        sp=x.get('surp'); f2=None if (isetf or sp is None) else (2 if sp>=8 else (1 if sp>=0 else 0))
        if r['ret26'] is None or r['vol'] in (None,0): f3=None; ra=None
        else:
            ra=round(r['ret26']/r['vol'],2)
            f3=3 if ra>=1.5 else (2 if ra>=0.8 else (1 if ra>=0.2 else 0))
            if r['pct200'] is not None:
                if r['pct200']>80: f3=max(0,f3-2)
                elif r['pct200']>50: f3=max(0,f3-1)
        revq=f.get('revq') or []
        if isetf or len(revq)<5 or revq[4]<=0: f4=None; revg=None
        else:
            revg=round((revq[0]/revq[4]-1)*100,1)
            f4=3 if revg>=40 else (2 if revg>=15 else (1 if revg>=5 else 0))
        gmd=f.get('gm_delta_bps'); f5=None if (isetf or gmd is None) else (2 if gmd>=300 else (1 if gmd>=0 else 0))
        fg=x.get('fwdrevg'); f6=None if (isetf or fg is None) else (2 if fg>=30 else (1 if fg>=10 else 0))
        ttm=sum(revq[:4]) if len(revq)>=4 else None
        if isetf or ttm in (None,0) or r['mcap'] is None: f7=None; evsg=None
        else:
            evs=(r['mcap']+(f.get('debt') or 0)-(f.get('cash') or 0))/ttm
            evsg=round(evs/max(fg if fg is not None else 3.0,3.0),2)
            f7=2 if evsg<0.3 else (1 if evsg<=0.8 else 0)
        dil=f.get('dil_yoy')
        if isetf or (f.get('cash') is None and dil is None): f8=None; bs='-'
        else:
            f8=2; parts=[]
            if dil is not None:
                parts.append(f'dil {dil:+.0f}%')
                if dil>10: f8-=2
                elif dil>3: f8-=1
            if f.get('cash') is not None and f.get('debt') is not None:
                parts.append('net cash' if f['cash']>f['debt'] else 'net debt')
                if f['cash']<f['debt'] and f['cash']>0 and f['debt']/max(f['cash'],1)>4: f8-=1
            f8=max(0,f8); bs='; '.join(parts) or '-'
        disp=x.get('disp'); nA=f.get('nA')
        f9=None if (isetf or disp is None or (nA is not None and nA<4)) else (2 if disp<40 else (1 if disp<=80 else 0))
        ins=f.get('insider_net_pct'); f10=None if (isetf or ins is None) else (2 if ins>0 else (1 if ins>=-5 else 0))
        sc={'F1':f1,'F2':f2,'F3':f3,'F4':f4,'F5':f5,'F6':f6,'F7':f7,'F8':f8,'F9':f9,'F10':f10}
        num=den=0
        for k in WT:
            if sc[k] is not None: num+=WT[k]*sc[k]/MX[k]; den+=WT[k]
        r.update(sc)
        niq=f.get('niq') or []; nidt=f.get('nidt') or []
        r['fy_rev']=f.get('fy_rev'); r['fy_profit']=f.get('fy_profit'); r['fy_year']=f.get('fy_year'); r['fcur']=f.get('fcur','USD')
        r['sector']=SECTOR.get(t) or f.get('industry') or f.get('ysector') or '\u2014'
        r['pq1']=qoq(niq[0],niq[1]) if len(niq)>=2 else None
        r['pq2']=qoq(niq[1],niq[2]) if len(niq)>=3 else None
        r['pq1lbl']=(f"{nidt[1]}\u2192{nidt[0]}" if len(nidt)>=2 else '')
        r['pq2lbl']=(f"{nidt[2]}\u2192{nidt[1]}" if len(nidt)>=3 else '')
        r['raw']={'rev90':rev90,'updn':f'{u}/{d}','surp':sp,'ra':ra,'revg':revg,'gmd':gmd,
                  'fwdrevg':fg,'evsg':evsg,'bs':bs,'disp':disp,'ins':ins}
        r['pct']=round(num/den*100,1) if den>0 else 0; r['wsc']=round(num,2); r['dataWt']=round(den,1)
        scored={k:sc[k] for k in WT if sc[k] is not None}
        if scored:
            twk=max(scored,key=lambda k:scored[k]/MX[k]); hwk=min(scored,key=lambda k:scored[k]/MX[k])
            r['tw']=TW[twk] if scored[twk]/MX[twk]>=0.5 else 'No standout strength'
            r['hw']=HW[hwk] if scored[hwk]/MX[hwk]<=0.5 else 'No major weakness'
        else:
            r['tw']=None; r['hw']=None
        if t in VETO: r['flag']='VETO'; r['vetoReason']=VETO[t]
        elif isetf: r['flag']='ETF'
        elif r['price'] is None or den==0: r['flag']='NO DATA'
        elif den < FULLWT: r['flag']='PARTIAL DATA'
        else: r['flag']='None'
        rows.append(r)
    scored_rows=[r for r in rows if r['flag'] in ('None','PARTIAL DATA')]
    scored_rows.sort(key=lambda r:(-r['pct'],-(r['raw']['ra'] or -9)))
    etf_rows=sorted([r for r in rows if r['flag']=='ETF'],key=lambda r:-r['pct'])
    veto_rows=[r for r in rows if r['flag']=='VETO']
    nod_rows=[r for r in rows if r['flag']=='NO DATA']
    ordered=scored_rows+etf_rows+veto_rows+nod_rows
    for i,r in enumerate(ordered,1): r['rank']=i
    return ordered

def main():
    print('fetching prices...'); P=fetch_prices()
    print('fetching fundamentals...'); F=fetch_fund()
    print('fetching estimates/targets...'); X=fetch_precise()
    print('scoring...'); rows=score(P,F,X)
    curs={r.get('fcur','USD') for r in rows if r.get('fcur') and r['fcur']!='USD'}
    fx={}
    for c in curs:
        try:
            h=yf.Ticker(f'{c}USD=X').history(period='5d')
            if len(h): fx[c]=float(h['Close'].iloc[-1])
        except Exception: pass
    for r in rows:
        c=r.get('fcur','USD')
        if c!='USD' and c in fx:
            for k in ('fy_rev','fy_profit'):
                if r.get(k) is not None: r[k]=round(r[k]*fx[c])
            r['fcur']='USD (from '+c+')'
    print('fetching news...'); news=fetch_news()
    for r in rows: r['news']=news.get(r['t'],[])
    now=dt.datetime.now(dt.timezone.utc)
    out={'meta':{'generated_utc':now.strftime('%Y-%m-%d %H:%M UTC'),
                 'universe':len(rows),'weights':WT,'maxes':MX,'fullwt':FULLWT},
         'stocks':rows}
    dst=os.path.join(os.path.dirname(os.path.abspath(__file__)),'data.json')
    json.dump(out,open(dst,'w'))
    print('wrote',dst,'-',len(rows),'stocks')
if __name__=='__main__': main()
