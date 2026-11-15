'use strict';
const $ = id => document.getElementById(id);
let current;
let accessToken='';
function forgetAccess(){accessToken='';current=undefined;$('access-token').value='';$('sign-in').hidden=false;$('sign-out').hidden=true;document.querySelectorAll('.page').forEach(page=>page.hidden=true);for(const id of ['kpis','channels','types','daily','customer-table','transaction-table','recon-table','transfer-kpis'])$(id).replaceChildren();$('release').textContent='Sign in to view certified data';$('certified').textContent='';$('bounds').textContent='';}
// Monetary JSON values are decimal strings. Format with BigInt, never Number.
function amount(value, currency='') {
  if(value===null || value===undefined) return 'Unavailable';
  const match=String(value).match(/^(-?)(\d+)(?:\.(\d+))?$/);
  if(!match) return String(value);
  return `${match[1]}${BigInt(match[2]).toLocaleString('en-US')}.${(match[3]||'').padEnd(2,'0')} ${currency}`.trim();
}
function el(tag, text, cls) {const x=document.createElement(tag);if(text!==undefined)x.textContent=text;if(cls)x.className=cls;return x;}
function table(id, rows, columns) {
  const box=$(id);box.replaceChildren();
  if(!rows.length){box.append(el('p','No data for this selection.','empty'));return;}
  const t=el('table'),head=el('tr');columns.forEach(c=>head.append(el('th',c[1])));const th=el('thead');th.append(head);t.append(th);const body=el('tbody');
  rows.forEach(row=>{const tr=el('tr');columns.forEach(([key,label,kind])=>{const raw=row[key];const td=el('td',kind==='money'?amount(raw):String(raw??''),kind==='money'?'money':'');if(raw==='PASS'||raw==='SUCCESS')td.classList.add('pass');if(raw==='FAIL'||raw==='FAILED')td.classList.add('fail');tr.append(td);});body.append(tr);});t.append(body);box.append(t);
}
function bars(id, rows) {
  const box=$(id);box.replaceChildren();const max=Math.max(...rows.map(r=>r.count),1);
  rows.forEach(r=>{const item=el('div'),top=el('div',undefined,'bar-top');top.append(el('span',r.name),el('strong',r.count.toLocaleString()));const track=el('div',undefined,'bar-track'),fill=el('div',undefined,'bar-fill');fill.style.width=`${r.count/max*100}%`;track.append(fill);item.append(top,track);box.append(item);});
  if(!rows.length)box.append(el('p','No events in this period.','empty'));
}
function customerTable(){const term=$('customer-search').value.toLowerCase();table('customer-table',current.customers.filter(r=>`${r.customer_id} ${r.masked_name}`.toLowerCase().includes(term)),[['customer_id','Customer'],['masked_name','Masked name'],['customer_segment','Segment'],['number_of_accounts','Accounts'],['number_of_cards','Cards'],['total_balance','Balance','money'],['txn_count_30d','Events 30d'],['card_spend_30d','Card spend 30d','money'],['failed_txn_count_30d','Failed 30d']]);}
function render(data) {
  current=data;const s=data.summary,f=data.filters,p=data.provenance;
  const active=document.querySelector('nav button.active');
  document.querySelectorAll('.page').forEach(page=>page.hidden=page.id!==active.dataset.page);
  $('release').textContent=`RELEASE ${p.release_id} · BUSINESS DATE ${p.as_of_date}`;
  $('certified').textContent=`SHA-256 VERIFIED · ${p.certified_at||'Certified'}`;
  $('bounds').textContent=`Certified interval ${f.min_date} — ${f.max_date} · ${p.code_revision||'Unrecorded revision'}`;
  const box=$('kpis');box.replaceChildren();
  const items=[['Transaction events',s.transactions.toLocaleString(),`${s.successful} successful · ${s.failed} failed`],['Posted transaction value',amount(s.posted_value,f.currency),'Successful core posting legs'],['Closing balance',amount(s.closing_balance,f.currency),`As of ${s.balance_date||'unavailable'} · ${s.balance_accounts} accounts`],['Reconciliation',s.reconciliation_groups?`${s.reconciliation_groups-s.reconciliation_failed} / ${s.reconciliation_groups} PASS`:'No data',s.reconciliation_failed?'Integrity exceptions detected':'Source / Bronze / Gold / GL'],['Requested transaction value',amount(s.transaction_value,f.currency),'All statuses · posting grain'],['Net posted movement',amount(s.net_posted_movement,f.currency),'Signed successful postings'],['Successful transfers',s.transfer_count.toLocaleString(),'One transfer, counted once'],['Transferred value',amount(s.transferred_value,f.currency),'Successful transfers only']];
  items.forEach(([label,value,detail])=>{const card=el('div',undefined,'kpi');card.append(el('span',label,'label'),el('strong',value,'value'),el('span',detail,'detail'));box.append(card);});
  bars('channels',data.channels);bars('types',data.types);const daily=$('daily');daily.replaceChildren();const max=Math.max(...data.daily.map(r=>r.count),1);data.daily.forEach(r=>{const day=el('div',undefined,'day'),bar=el('div',undefined,'day-bar');bar.style.height=`${r.count/max*100}%`;day.title=`${r.date}: ${r.count} events`;day.append(bar);daily.append(day);});if(!data.daily.length)daily.append(el('p','No events in this period.','empty'));
  $('customer-note').textContent=`Fixed certified snapshot ${data.customer_as_of_date} · 30-day window · ${f.currency}. Transaction date filters do not change this snapshot.`;customerTable();
  const transfer=$('transfer-kpis');transfer.replaceChildren();[['Transfer count',s.transfer_count],['Transferred value',amount(s.transferred_value,f.currency)]].forEach(([label,value])=>{const x=el('div');x.append(el('span',label),el('strong',String(value)));transfer.append(x);});
  table('transaction-table',data.transactions,[['business_date','Date'],['transaction_id','Transaction'],['transaction_type','Type'],['status','Status'],['channel','Channel'],['amount','Requested','money'],['signed_amount','Net posted','money'],['segment','Historical segment']]);
  $('recon-status').textContent=s.reconciliation_groups?(s.reconciliation_failed?'FAIL':'ALL GROUPS PASS'):'NO DATA';
  table('recon-table',data.reconciliation,[['business_date','Date'],['currency','Currency'],['source_count','Source'],['lakehouse_count','Bronze'],['gold_count','Gold'],['source_amount','Source amount','money'],['gold_amount','Gold amount','money'],['gl_debit','GL debit','money'],['gl_credit','GL credit','money'],['unmatched_transactions','Unmatched'],['reconciliation_status','Status']]);
}
async function load(initial=false){$('apply').disabled=true;$('error').hidden=true;$('status').textContent='Verifying certified exports…';try{const params=initial?'':`?${new URLSearchParams({currency:$('currency').value,start:$('start').value,end:$('end').value})}`;const headers=accessToken?{Authorization:`Bearer ${accessToken}`}:{},response=await fetch(`/api/report${params}`,{cache:'no-store',headers});const data=await response.json();if(response.status===401){forgetAccess();$('status').textContent='Sign in required';return;}if(!response.ok)throw Error(data.error||'Unable to read certified data');if(initial){$('currency').replaceChildren(...data.filters.currencies.map(c=>{const o=el('option',c);o.value=c;return o;}));$('currency').value=data.filters.currency;for(const id of ['start','end']){$(id).min=data.filters.min_date;$(id).max=data.filters.max_date;$(id).value=data.filters[id];}}render(data);$('sign-in').hidden=true;$('sign-out').hidden=!accessToken;$('status').textContent='Certified bundle verified';}catch(error){$('error').textContent=error.message;$('error').hidden=false;$('status').textContent='Data unavailable';document.querySelectorAll('.page').forEach(p=>p.hidden=true);}finally{$('apply').disabled=false;}}
document.querySelectorAll('nav button').forEach(button=>button.addEventListener('click',()=>{document.querySelectorAll('nav button').forEach(b=>b.classList.toggle('active',b===button));document.querySelectorAll('.page').forEach(p=>p.hidden=p.id!==button.dataset.page);}));
$('apply').addEventListener('click',()=>load());$('customer-search').addEventListener('input',()=>{if(current)customerTable();});load(true);
$('sign-in-form').addEventListener('submit',event=>{event.preventDefault();accessToken=$('access-token').value.trim();$('access-token').value='';load(true);});$('sign-out').addEventListener('click',()=>{forgetAccess();$('status').textContent='Signed out';});
