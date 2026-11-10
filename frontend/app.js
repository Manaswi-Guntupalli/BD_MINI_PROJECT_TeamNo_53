const form = document.getElementById('search-form');
const input = document.getElementById('query');
const results = document.getElementById('results');
const title = document.getElementById('results-title');
const meta = document.getElementById('meta');
const status = document.getElementById('status');
const version = document.getElementById('version');
function elt(tag, className, text) { const x=document.createElement(tag); if(className)x.className=className;if(text!==undefined)x.textContent=text;return x; }
async function check() { try { const r=await fetch('/api/health'); const d=await r.json();status.textContent=d.ready?'● Index online':'● No index deployed';version.textContent='Index '+(d.version||'—'); }catch{status.textContent='● Service unavailable';} }
async function search(query) {
  results.replaceChildren(elt('div','empty','Searching…'));
  const started=performance.now();
  try {
    const res=await fetch(`/api/search?q=${encodeURIComponent(query)}&limit=20`);
    const data=await res.json();
    if(!res.ok)throw new Error(data.detail||'Search failed');
    title.textContent=`Results for “${query}”`;
    meta.textContent=`${data.count} results · ${(performance.now()-started).toFixed(0)} ms · ${data.index_version}`;
    results.replaceChildren();
    if(!data.results.length)results.append(elt('div','empty','No matching documents found. Try a broader query.'));
    for(const item of data.results){
      const article=elt('article','item');
      article.append(elt('div','url',item.url||'Document '+item.doc_id));
      const heading=elt('h3');const link=elt('a',null,item.title);link.href=/^https?:\/\//i.test(item.url)?item.url:'#';link.rel='noopener noreferrer';link.target='_blank';heading.append(link);article.append(heading);
      article.append(elt('p',null,item.snippet),elt('div','score',`SCORE ${item.score.toFixed(4)} · BM25 ${item.bm25.toFixed(4)} · PAGERANK ${item.pagerank.toFixed(6)}`));
      results.append(article);
    }
  }catch(e){results.replaceChildren(elt('div','empty',e.message));meta.textContent='Search unavailable';}
}
form.addEventListener('submit',e=>{e.preventDefault();if(input.value.trim())search(input.value.trim());});
document.querySelectorAll('[data-query]').forEach(b=>b.addEventListener('click',()=>{input.value=b.dataset.query;search(input.value);}));check();
