import React, { useState, useEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { getTypeStyle } from '../utils/theme';
import GenericTable from '../components/GenericTable';
import api, { API } from '../utils/api';

const obtainErrorMsg = (e) => e.response?.data?.error || e.message || String(e);

export default function Explore() {
  const location = useLocation();
  const navigate = useNavigate();
  const selConn = location.state?.selConn || null;

  const [dbs, setDbs] = useState([]);
  const [selDb, setSelDb] = useState(null);
  const [tables, setTables] = useState([]);
  const [selTable, setSelTable] = useState(null);
  
  const [schemaData, setSchemaData] = useState({ rows: [], lastUpdated: null });
  const [dbSearch, setDbSearch] = useState('');
  const [colSearch, setColSearch] = useState('');
  
  const [explorerTab, setExplorerTab] = useState('dbs'); 
  const [globalColSearch, setGlobalColSearch] = useState('');
  const [globalColResults, setGlobalColResults] = useState([]);
  const [isSearchingGlobal, setIsSearchingGlobal] = useState(false);
  
  const [loading, setLoading] = useState({ dbs: false, tables: false, schema: false, export: false, metaSave: false, obs: false, preview: false, workbench: false, details: false });

  const [tableMeta, setTableMeta] = useState({ description: '', use_case: '', column_comments: {} });
  const [editMeta, setEditMeta] = useState(false);
  const [metaForm, setMetaForm] = useState({ description: '', use_case: '', column_comments: {} });
  const [generatingAI, setGeneratingAI] = useState(false);

  const [tableDetails, setTableDetails] = useState({ totalRows: null, tableSize: 'N/A', numFiles: 0, hudiConfig: null });

  const [mainTab, setMainTab] = useState('schema'); 
  const [obsHistory, setObsHistory] = useState([]);
  const [previewData, setPreviewData] = useState(null);
  const [columnProfiles, setColumnProfiles] = useState({});
  const [profilingCol, setProfilingCol] = useState(null);
  const [isExplorerOpen, setIsExplorerOpen] = useState(true);

  const [workbenchQuery, setWorkbenchQuery] = useState('');
  const [workbenchResults, setWorkbenchResults] = useState(null);
  const [workbenchError, setWorkbenchError] = useState('');
  const [editorHeight, setEditorHeight] = useState(160);
  const [isDragging, setIsDragging] = useState(false);
  const lineNumbersRef = useRef(null);

  const [hasAutoRoutedDb, setHasAutoRoutedDb] = useState(false);
  const [hasAutoRoutedTable, setHasAutoRoutedTable] = useState(false);

  const fetchObsHistory = async () => {
      if (!selConn) return;
      try { const res = await api.post(`${API}/observability/history`, { connection_id: selConn.id, db_name: selDb, table_name: selTable }); setObsHistory(res.data); } 
      catch (err) { alert(`Failed to load history: ${obtainErrorMsg(err)}`); }
  };

  useEffect(() => {
    if (selConn) {
        setLoading(prev => ({ ...prev, dbs: true }));
        api.post(`${API}/explore/fetch`, { id: selConn.id, type: 'databases' })
             .then(res => setDbs(res.data))
             .catch(e => { alert(`Error:\n\n${obtainErrorMsg(e)}`); navigate('/connections'); })
             .finally(() => setLoading(prev => ({ ...prev, dbs: false })));
    }
  }, [selConn?.id]);

  useEffect(() => { 
      if (mainTab === 'observability' && selTable && selConn) { fetchObsHistory(); } 
  }, [mainTab, selTable, selConn]);

  const handleDbClick = async (db) => {
    if (selDb === db) { setSelDb(null); setTables([]); setSelTable(null); setSchemaData({ rows: [], lastUpdated: null }); setColSearch(''); setMainTab('schema'); return; }
    setSelDb(db); setSelTable(null); setSchemaData({ rows: [], lastUpdated: null }); setTables([]); setColSearch('');
    setTableMeta({ description: '', use_case: '', column_comments: {} }); setEditMeta(false); setMainTab('schema');
    setLoading(prev => ({ ...prev, tables: true }));
    try { const res = await api.post(`${API}/explore/fetch`, { id: selConn.id, type: 'tables', db }); setTables(res.data); } 
    catch (e) { alert(`Error:\n\n${obtainErrorMsg(e)}`); }
    setLoading(prev => ({ ...prev, tables: false }));
  };

  const handleTableClick = async (tName, overrideDb = null, autoHighlightCol = '') => {
    const destinationDb = overrideDb || selDb;
    if (overrideDb) { setSelDb(destinationDb); setTables([]); }
    setSelTable(tName); setColSearch(autoHighlightCol);
    setEditMeta(false); setTableMeta({ description: '', use_case: '', column_comments: {} });
    setPreviewData(null); setColumnProfiles({}); setProfilingCol(null); setMainTab('schema'); 
    setWorkbenchQuery(`SELECT * FROM \`${destinationDb}\`.\`${tName}\` LIMIT 10`);
    
    setLoading(prev => ({ ...prev, schema: true, details: true }));
    setTableDetails({ totalRows: null, tableSize: 'N/A', numFiles: 0, hudiConfig: null });

    try {
        const resSchema = await api.post(`${API}/explore/fetch`, { id: selConn.id, type: 'schema', db: destinationDb, table: tName });
        setSchemaData({ rows: resSchema.data.schema, lastUpdated: resSchema.data.last_updated });
        
        const resMeta = await api.post(`${API}/table-metadata/retrieve`, { connection_id: selConn.id, db_name: destinationDb, table_name: tName });
        const metaPayload = { description: resMeta.data.description || '', use_case: resMeta.data.use_case || '', column_comments: resMeta.data.column_comments || {} };
        setTableMeta(metaPayload); setMetaForm(metaPayload);
    } catch (e) { alert(`Error:\n\n${obtainErrorMsg(e)}`); }
    setLoading(prev => ({ ...prev, schema: false }));

    try {
        const resDetails = await api.post(`${API}/explore/table-details`, { connection_id: selConn.id, db_name: destinationDb, table_name: tName });
        setTableDetails(resDetails.data);
    } catch(e) {}
    setLoading(prev => ({ ...prev, details: false }));
  };

  const autoDb = location.state?.autoDb || null;
  const autoTable = location.state?.autoTable || null;

  useEffect(() => {
      if (autoDb && dbs.length > 0 && !hasAutoRoutedDb) {
          const targetDb = dbs.find(d => d.toLowerCase() === autoDb.toLowerCase());
          if (targetDb) {
              setTimeout(() => handleDbClick(targetDb), 0);
              setHasAutoRoutedDb(true);
          }
      }
  }, [dbs, autoDb, hasAutoRoutedDb]);

  useEffect(() => {
      if (autoTable && tables.length > 0 && !hasAutoRoutedTable) {
          const targetTable = tables.find(t => t.toLowerCase() === autoTable.toLowerCase());
          if (targetTable) {
              setTimeout(() => handleTableClick(targetTable, autoDb), 0);
              setHasAutoRoutedTable(true);
          }
      }
  }, [tables, autoTable, hasAutoRoutedTable]);

  const performGlobalSearch = async (e) => {
      e.preventDefault();
      if (!globalColSearch.trim()) return;
      setIsSearchingGlobal(true);
      try { const res = await api.post(`${API}/search/fetch`, { id: selConn.id, col: globalColSearch }); setGlobalColResults(res.data); } 
      catch (err) { alert(`Search failed: ${obtainErrorMsg(err)}`); }
      setIsSearchingGlobal(false);
  };

  const downloadExcel = async (db) => {
    setLoading(prev => ({ ...prev, export: true }));
    try {
        const response = await api.post(`${API}/export/fetch`, { id: selConn.id, db: db }, { responseType: 'blob' });
        const url = window.URL.createObjectURL(new Blob([response.data]));
        const link = document.createElement('a'); link.href = url; link.setAttribute('download', `${db}_data_dictionary.xlsx`);
        document.body.appendChild(link); link.click(); link.remove();
    } catch (e) { alert(`Failed to export: ${e.message}`); }
    setLoading(prev => ({ ...prev, export: false }));
  };

  const saveMetadata = async () => {
      setLoading(prev => ({ ...prev, metaSave: true }));
      try {
          await api.post(`${API}/table-metadata/save`, { connection_id: selConn.id, db_name: selDb, table_name: selTable, description: metaForm.description, use_case: metaForm.use_case, column_comments: metaForm.column_comments });
          setTableMeta(metaForm); setEditMeta(false);
      } catch (err) { alert(`Failed to save metadata: ${obtainErrorMsg(err)}`); }
      setLoading(prev => ({ ...prev, metaSave: false }));
  };

  const generateMetadataWithAI = async () => {
      setGeneratingAI(true);
      try {
          const res = await api.post(`${API}/table-metadata/generate`, { connection_id: selConn.id, db_name: selDb, table_name: selTable, schema: schemaData.rows });
          setMetaForm({ description: res.data.description || '', use_case: res.data.use_case || '', column_comments: res.data.column_comments || {} });
      } catch (err) { alert(`AI Generation failed: ${obtainErrorMsg(err)}`); }
      setGeneratingAI(false);
  };

  const fetchPreview = async () => {
      if (previewData) return;
      setLoading(prev => ({ ...prev, preview: true }));
      try { const res = await api.post(`${API}/explore/preview`, { connection_id: selConn.id, db_name: selDb, table_name: selTable }); setPreviewData(res.data); } 
      catch (err) { alert(`Failed to load data preview: ${obtainErrorMsg(err)}`); setPreviewData([]); }
      setLoading(prev => ({ ...prev, preview: false }));
  };

  const profileColumn = async (colName) => {
      setProfilingCol(colName);
      try { const res = await api.post(`${API}/explore/profile-column`, { connection_id: selConn.id, db_name: selDb, table_name: selTable, column_name: colName }); setColumnProfiles(prev => ({ ...prev, [colName]: res.data })); } 
      catch (err) { alert(`Profiling failed: ${obtainErrorMsg(err)}`); }
      setProfilingCol(null);
  };

  const runProfiler = async () => {
      setLoading(prev => ({ ...prev, obs: true }));
      try { await api.post(`${API}/observability/calculate`, { connection_id: selConn.id, db_name: selDb, table_name: selTable }); await fetchObsHistory(); } 
      catch (err) { alert(`Profiling Failed: ${obtainErrorMsg(err)}`); }
      setLoading(prev => ({ ...prev, obs: false }));
  };

  const executeWorkbenchQuery = async () => {
      setWorkbenchError('');
      setWorkbenchResults(null);
      setLoading(prev => ({ ...prev, workbench: true }));
      try {
          const res = await api.post(`${API}/explore/query`, { connection_id: selConn.id, db_name: selDb, query: workbenchQuery });
          setWorkbenchResults(res.data);
      } catch (err) {
          setWorkbenchError(err.response?.data?.error || err.message);
      }
      setLoading(prev => ({ ...prev, workbench: false }));
  };

  const startDrag = (e) => {
      e.preventDefault();
      const startY = e.clientY;
      const startHeight = editorHeight;

      const doDrag = (dragEvent) => {
          const newHeight = startHeight + (dragEvent.clientY - startY);
          if (newHeight >= 80 && newHeight <= window.innerHeight * 0.75) {
              setEditorHeight(newHeight);
          }
      };

      const stopDrag = () => {
          document.removeEventListener('mousemove', doDrag);
          document.removeEventListener('mouseup', stopDrag);
          setIsDragging(false);
      };

      setIsDragging(true);
      document.addEventListener('mousemove', doDrag);
      document.addEventListener('mouseup', stopDrag);
  };

  const filteredDbs = dbs.filter(db => db.toLowerCase().includes(dbSearch.toLowerCase()));
  const filteredSchema = schemaData.rows.filter(row => {
      if (!colSearch) return true;
      const term = colSearch.toLowerCase();
      return Object.values(row).some(val => String(val).toLowerCase().includes(term));
  });

  const renderObservabilityCards = () => {
      if (obsHistory.length === 0) return null;
      const latest = obsHistory[0];
      let todayStr = "TODAY"; let prevStr = "PREVIOUS";

      if (latest.target_date) {
          const measureDate = new Date(latest.target_date);
          todayStr = measureDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
          const prevDate = new Date(measureDate); prevDate.setDate(prevDate.getDate() - 1);
          prevStr = prevDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }); 
      }

      const inc = latest.today_count !== null ? Number(latest.today_count) : null;
      let incStyle = { color: 'text-gray-700', bg: 'bg-white', border: 'border-gray-200', icon: 'fa-minus', label: 'No Data Analyzed' };
      if (inc !== null) {
          if (inc > 0) incStyle = { color: 'text-emerald-700', bg: 'bg-emerald-50/50', border: 'border-emerald-200', icon: 'fa-arrow-trend-up', label: 'Active Ingestion' };
          else if (inc === 0) incStyle = { color: 'text-amber-600', bg: 'bg-amber-50/50', border: 'border-amber-200', icon: 'fa-exclamation-triangle', label: 'Stale / No Ingestion' };
          else if (inc < 0) incStyle = { color: 'text-rose-600', bg: 'bg-rose-50/50', border: 'border-rose-200', icon: 'fa-arrow-trend-down', label: 'Records Deleted' };
      }

      return (
          <div className="grid grid-cols-3 gap-6 mb-8 fade-in">
              <div className="bg-white p-6 rounded-xl border border-gray-200 shadow-sm flex flex-col items-center justify-center text-center hover:-translate-y-1 hover:shadow-md transition-all duration-300">
                  <span className="text-[11px] font-bold text-gray-400 uppercase tracking-widest mb-2"><i className="fas fa-history mr-1"></i> Previous (Until {prevStr})</span>
                  <span className="text-3xl font-extrabold text-gray-700">{latest.previous_count !== null ? Number(latest.previous_count).toLocaleString() : 'N/A'}</span>
              </div>
              <div className={`${incStyle.bg} p-6 rounded-xl border ${incStyle.border} shadow-sm flex flex-col items-center justify-center text-center hover:-translate-y-1 hover:shadow-md transition-all duration-300 relative overflow-hidden`}>
                  <div className={`absolute -right-4 -top-4 opacity-10 text-6xl ${incStyle.color}`}><i className={`fas ${incStyle.icon}`}></i></div>
                  <span className={`text-[11px] font-bold ${incStyle.color} uppercase tracking-widest mb-2 flex items-center gap-1.5`}><i className={`fas ${incStyle.icon}`}></i> {incStyle.label} ({todayStr})</span>
                  <span className={`text-4xl font-black ${incStyle.color}`}>{inc !== null ? (inc > 0 ? `+${inc.toLocaleString()}` : inc.toLocaleString()) : 'N/A'}</span>
              </div>
              <div className="bg-indigo-50/50 p-6 rounded-xl border border-indigo-200 shadow-sm flex flex-col items-center justify-center text-center hover:-translate-y-1 hover:shadow-md transition-all duration-300">
                  <span className="text-[11px] font-bold text-indigo-600 uppercase tracking-widest mb-2"><i className="fas fa-database mr-1"></i> Total (As Of {todayStr})</span>
                  <span className="text-3xl font-extrabold text-indigo-700">{Number(latest.total_count).toLocaleString()}</span>
              </div>
          </div>
      );
  };

  const renderTrendChart = () => {
      if (!obsHistory || obsHistory.length === 0) return null;
      const chronologicalData = [...obsHistory].reverse();
      const chartData = chronologicalData.map((run, idx, arr) => {
          let incremental = 0;
          if (run.today_count !== null) incremental = Number(run.today_count);
          else if (idx > 0) incremental = Number(run.total_count) - Number(arr[idx-1].total_count);
          const dateObj = run.target_date ? new Date(run.target_date) : new Date(run.measured_at);
          return { displayValue: Math.max(0, incremental), actualValue: incremental, label: dateObj.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) };
      });
      const maxVal = Math.max(...chartData.map(d => d.displayValue), 10);
      return (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-8 flex flex-col shrink-0 fade-in">
              <h3 className="text-[11px] font-bold text-gray-500 uppercase tracking-widest flex items-center gap-2 mb-6"><i className="fas fa-chart-area text-indigo-400"></i> Daily Ingestion Trend (New Records)</h3>
              <div className="h-44 flex items-end justify-between gap-1 w-full relative border-b border-gray-100 pb-2">
                  {chartData.map((d, i) => {
                      const heightPct = (d.displayValue / maxVal) * 100;
                      return (
                          <div key={i} className="flex flex-col items-center justify-end flex-1 group relative h-full cursor-crosshair">
                              <div className="absolute -top-10 bg-gray-800 text-white text-[11px] font-mono px-2.5 py-1.5 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap z-10 pointer-events-none shadow-lg">
                                  <span className="text-gray-300 mr-2 font-sans">{d.label}:</span>{d.actualValue > 0 ? '+' : ''}{d.actualValue.toLocaleString()}
                              </div>
                              <div className="w-full max-w-[32px] bg-indigo-100 group-hover:bg-indigo-500 transition-colors rounded-t-sm" style={{ height: `${Math.max(1, heightPct)}%` }}></div>
                              {(chartData.length <= 15 || i % 2 === 0 || i === chartData.length - 1) ? <span className="text-[9px] text-gray-400 mt-3 rotate-45 origin-top-left absolute -bottom-8 whitespace-nowrap">{d.label}</span> : null}
                          </div>
                      );
                  })}
              </div>
          </div>
      );
  };

  if (!selConn) {
      return (
          <div className="flex-1 flex flex-col items-center justify-center text-gray-400 fade-in bg-white rounded-3xl shadow-sm border border-gray-200 border-dashed m-4 p-8">
              <div className="w-24 h-24 bg-gray-50 rounded-full flex items-center justify-center mb-6 shadow-inner border border-gray-100"><i className="fas fa-compass text-4xl text-gray-300"></i></div>
              <h2 className="text-2xl font-bold text-gray-700 mb-2">No Connection Selected</h2>
              <p className="text-base text-gray-500 max-w-md text-center mb-6">Navigate to the Connections tab or use the Global Search to select a data source to explore.</p>
              <button onClick={() => navigate('/connections')} className="bg-indigo-600 text-white px-6 py-3 rounded-lg font-bold shadow-md hover:bg-indigo-700 transition-colors">Go to Connections</button>
          </div>
      );
  }

  return (
      <div className="flex-1 flex flex-col fade-in h-full overflow-hidden">
        <header className="bg-white px-8 flex justify-between items-center z-10 h-20 border-b border-gray-200 shrink-0">
            <div className="flex items-center gap-5">
                <button onClick={() => setIsExplorerOpen(!isExplorerOpen)} className={`text-gray-400 hover:text-indigo-600 focus:outline-none w-9 h-9 flex items-center justify-center rounded-xl hover:bg-indigo-50 transition-all border border-transparent hover:border-indigo-100 ${!isExplorerOpen ? 'bg-indigo-50 text-indigo-600 border-indigo-100 shadow-sm' : ''}`}>
                    <i className={`fas fa-chevron-${isExplorerOpen ? 'left' : 'right'} text-sm`}></i>
                </button>
                <div>
                    <h1 className="text-2xl font-extrabold text-gray-800 capitalize tracking-tight flex items-center gap-3">{selConn.name} <span className="bg-indigo-50 text-indigo-700 text-[10px] px-2.5 py-1 rounded-md font-bold uppercase tracking-widest border border-indigo-200 shadow-sm">{selConn.type}</span></h1>
                    <p className="text-[11px] text-gray-400 mt-1 font-mono tracking-wider font-semibold uppercase">HOST: {selConn.masked_host || selConn.host}</p>
                </div>
            </div>
        </header>

        <div className="flex-1 flex overflow-hidden">
            <div className={`bg-gray-50 border-r flex flex-col overflow-hidden shrink-0 transition-all duration-300 ease-in-out ${isExplorerOpen ? 'w-[320px] opacity-100 border-gray-200' : 'w-0 opacity-0 border-transparent m-0'}`}>
                <div className="p-5 bg-white border-b border-gray-100 flex flex-col gap-4 shrink-0">
                    <div className="flex justify-between items-center px-1"><span className="font-bold text-gray-800 text-sm uppercase tracking-widest flex items-center gap-2"><i className="fas fa-compass text-indigo-500"></i> Explorer</span></div>
                    <div className="flex bg-gray-100 p-1.5 rounded-xl shadow-inner">
                        <button onClick={() => setExplorerTab('dbs')} className={`flex-1 text-xs font-bold py-2 rounded-lg transition-all duration-200 ${explorerTab === 'dbs' ? 'bg-white shadow-sm text-indigo-700' : 'text-gray-500 hover:text-gray-700 hover:bg-gray-200/50'}`}>Databases</button>
                        <button onClick={() => setExplorerTab('cols')} className={`flex-1 text-xs font-bold py-2 rounded-lg transition-all duration-200 ${explorerTab === 'cols' ? 'bg-white shadow-sm text-indigo-700' : 'text-gray-500 hover:text-gray-700 hover:bg-gray-200/50'}`}>Global Columns</button>
                    </div>
                    {explorerTab === 'dbs' ? (
                        <div className="relative group"><i className="fas fa-search absolute left-3.5 top-1/2 transform -translate-y-1/2 text-gray-400 text-sm"></i><input type="text" placeholder="Search databases..." value={dbSearch} onChange={(e) => setDbSearch(e.target.value)} className="w-full bg-gray-50 border border-gray-200 text-gray-800 text-sm rounded-xl pl-10 pr-4 py-2.5 focus:outline-none focus:bg-white focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100/50 transition-all placeholder-gray-400 font-medium" /></div>
                    ) : (
                        <form onSubmit={performGlobalSearch} className="relative group"><i className="fas fa-search absolute left-3.5 top-1/2 transform -translate-y-1/2 text-indigo-400 text-sm"></i><input type="text" placeholder="Search all columns..." value={globalColSearch} onChange={(e) => setGlobalColSearch(e.target.value)} className="w-full bg-indigo-50/50 border border-indigo-200 text-indigo-900 text-sm rounded-xl pl-10 pr-4 py-2.5 focus:outline-none focus:bg-white focus:border-indigo-500 focus:ring-2 focus:ring-indigo-100 transition-all placeholder-indigo-400/70 font-medium" /></form>
                    )}
                </div>
            
                <div className="flex-1 overflow-y-auto p-3 custom-scrollbar bg-gray-50/30">
                  {explorerTab === 'dbs' && (
                      <>
                        {loading.dbs && <div className="p-10 text-center text-indigo-500 text-sm flex flex-col items-center justify-center gap-4"><i className="fas fa-circle-notch fa-spin text-3xl"></i><span className="font-bold tracking-wide">Fetching Databases...</span></div>}
                        {!loading.dbs && filteredDbs.length === 0 && dbs.length > 0 && <div className="text-center text-gray-400 text-sm py-10 italic">No databases match "{dbSearch}"</div>}

                        {filteredDbs.map(db => (
                          <div key={db} className="mb-1.5">
                            <div onClick={() => handleDbClick(db)} className={`p-2.5 rounded-lg cursor-pointer flex items-center justify-between transition-colors group ${selDb === db ? 'bg-indigo-50 text-indigo-700 font-medium border border-indigo-100 shadow-sm' : 'hover:bg-white hover:shadow-sm text-gray-600 font-normal border border-transparent'}`}>
                              <span className="flex items-center gap-2.5 text-[13px]"><i className={`fas fa-database ${selDb === db ? 'text-indigo-500' : 'text-gray-400'}`}></i><span className="truncate max-w-[160px]">{db}</span></span>
                              <i className={`fas fa-chevron-right text-[10px] transition-transform duration-300 ${selDb === db ? 'rotate-90 text-indigo-600' : 'text-gray-300'}`}></i> 
                            </div>
                            
                            {selDb === db && (
                              <div className="mt-2 ml-4 pl-4 border-l-2 border-indigo-100 py-2 space-y-1 fade-in">
                                <div className="mb-2 flex justify-between items-center px-1">
                                  <span className="text-[9px] font-extrabold text-gray-400 uppercase tracking-widest">Tables</span>
                                  <button onClick={() => downloadExcel(db)} disabled={loading.export} className="text-[9px] bg-white text-emerald-600 border border-gray-200 hover:border-emerald-200 px-2 py-1 rounded shadow-sm flex items-center gap-1 transition-all disabled:opacity-50 font-bold uppercase"><i className="fas fa-file-excel"></i> Export</button>
                                </div>
                                {loading.tables ? <div className="p-2 text-[11px] text-gray-400 flex items-center gap-2"><i className="fas fa-spinner fa-spin text-indigo-500"></i> Loading...</div> : (
                                  tables.map(tName => (
                                    <div key={tName} onClick={() => handleTableClick(tName)} className={`px-2 py-1.5 cursor-pointer rounded-md transition-all ${selTable === tName ? 'bg-indigo-600 text-white shadow-md font-bold' : 'text-gray-500 hover:bg-white hover:text-gray-800 text-xs font-normal'}`}>
                                      <div className="flex items-center gap-2"><i className={`fas fa-table text-[10px] ${selTable === tName ? 'text-indigo-200' : 'text-gray-400'}`}></i><span className="truncate">{tName}</span></div>
                                    </div>
                                  ))
                                )}
                              </div>
                            )}
                          </div>
                        ))}
                      </>
                  )}
                  {explorerTab === 'cols' && (
                      <div className="fade-in">
                          {isSearchingGlobal && <div className="p-6 text-center text-indigo-500 text-xs flex flex-col items-center gap-2"><i className="fas fa-circle-notch fa-spin text-lg"></i><span className="font-bold">Searching...</span></div>}
                          {!isSearchingGlobal && globalColResults.map((res, i) => (
                              <div key={i} onClick={() => handleTableClick(res.tbl, res.db, res.col)} className="p-3 mb-2 bg-white border border-gray-200 rounded-lg hover:border-indigo-400 hover:shadow-md cursor-pointer shadow-sm group transition-all">
                                  <div className="text-xs font-bold text-indigo-600 flex items-center gap-2 mb-1.5"><i className="fas fa-columns opacity-50"></i> {res.col}</div>
                                  <div className="text-[10px] text-gray-500 flex items-center gap-1.5 font-mono bg-gray-50 p-1 rounded"><i className="fas fa-database text-gray-400"></i> <span className="truncate">{res.db}</span></div>
                                  <div className="text-[10px] text-gray-500 flex items-center gap-1.5 mt-1 font-mono bg-gray-50 p-1 rounded ml-3"><i className="fas fa-level-up-alt rotate-90 text-gray-300"></i> <span className="truncate">{res.tbl}</span></div>
                              </div>
                          ))}
                      </div>
                  )}
                </div>
            </div>

            <div className="flex-1 flex flex-col min-w-0 bg-white">
              <div className="flex border-b border-gray-200 px-6 bg-white shrink-0">
                  <button onClick={() => setMainTab('schema')} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all ${mainTab === 'schema' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-columns mr-2"></i>Schema & Context</button>
                  <button onClick={() => { setMainTab('preview'); fetchPreview(); }} disabled={!selTable} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all disabled:opacity-30 ${mainTab === 'preview' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-table mr-2"></i>Data Preview</button>
                  <button onClick={() => setMainTab('profiling')} disabled={!selTable} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all disabled:opacity-30 ${mainTab === 'profiling' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-search-plus mr-2"></i>Data Profiling</button>
                  <button onClick={() => setMainTab('observability')} disabled={!selTable} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all disabled:opacity-30 ${mainTab === 'observability' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-chart-line mr-2"></i>Data Observability</button>
                  <button onClick={() => setMainTab('workbench')} className={`py-4 px-5 text-[13px] font-bold uppercase tracking-wide border-b-[3px] transition-all ${mainTab === 'workbench' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-gray-500 hover:text-gray-800'}`}><i className="fas fa-terminal mr-2"></i>SQL Workbench</button>
              </div>

              {mainTab === 'schema' && (
                  <div className="flex-1 flex flex-col overflow-y-auto custom-scrollbar fade-in bg-gray-50/30">
                      {selTable ? (
                          <div>
                              <div className="p-8 shrink-0">
                                  <div className="flex justify-between items-center mb-4"><h3 className="text-sm font-bold text-gray-800 flex items-center gap-2"><i className="fas fa-book-open text-indigo-500"></i> Business Context</h3><button onClick={() => setEditMeta(!editMeta)} className="text-xs font-semibold text-indigo-600 hover:text-indigo-800 transition-colors bg-indigo-50 hover:bg-indigo-100 px-4 py-1.5 rounded-lg border border-indigo-200 shadow-sm">{editMeta ? 'Cancel Editing' : 'Edit Documentation'}</button></div>
                                  {editMeta ? (
                                      <div className="space-y-3 fade-in bg-white p-5 rounded-xl border border-gray-200 shadow-sm">
                                          <div className="flex justify-between items-center mb-1"><span className="text-xs font-bold text-gray-600 uppercase tracking-widest"><i className="fas fa-pen mr-1"></i> Editor</span><button type="button" onClick={generateMetadataWithAI} disabled={generatingAI || loading.schema} className="text-[10px] bg-gradient-to-r from-purple-50 to-fuchsia-50 text-purple-700 hover:from-purple-100 border border-purple-200 px-3 py-1.5 rounded-md shadow-sm flex items-center gap-1.5 transition-colors disabled:opacity-50 font-bold uppercase tracking-wider">{generatingAI ? <i className="fas fa-circle-notch fa-spin"></i> : <i className="fas fa-magic text-purple-500"></i>} {generatingAI ? 'Analyzing...' : 'Auto-Draft with AI'}</button></div>
                                          <input type="text" placeholder="Brief Table Description..." className="w-full text-xs border border-gray-300 rounded-lg px-4 py-2.5 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition-all" value={metaForm.description} onChange={(e) => setMetaForm({...metaForm, description: e.target.value})} />
                                          <textarea placeholder="What is the primary business use case for this data?" className="w-full text-xs border border-gray-300 rounded-lg px-4 py-2.5 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition-all h-20 resize-none" value={metaForm.use_case} onChange={(e) => setMetaForm({...metaForm, use_case: e.target.value})} />
                                          <div className="flex justify-end pt-2"><button onClick={saveMetadata} disabled={loading.metaSave} className="bg-indigo-600 text-white text-xs font-bold px-6 py-2.5 rounded-lg hover:bg-indigo-700 transition-all duration-300 shadow-md hover:-translate-y-0.5">{loading.metaSave ? <><i className="fas fa-spinner fa-spin mr-2"></i>Saving...</> : 'Save Documentation'}</button></div>
                                      </div>
                                  ) : (
                                      <div className="space-y-2 bg-orange-50/30 p-4 rounded-xl border border-orange-100 shadow-sm">
                                          <p className="text-xs text-orange-600 leading-relaxed"><span className="font-semibold text-gray-700 mr-2 uppercase tracking-wide">Description:</span> {tableMeta.description || <span className="italic text-orange-400">No description provided.</span>}</p>
                                          <p className="text-xs text-orange-600 leading-relaxed"><span className="font-semibold text-gray-700 mr-2 uppercase tracking-wide">Use Case:</span> {tableMeta.use_case || <span className="italic text-orange-400">No use case documented.</span>}</p>
                                      </div>
                                  )}
                              </div>
                              <div className="flex flex-col min-w-0 bg-white border-t border-gray-200 flex-1">
                                  <div className="px-6 py-4 border-b border-gray-100 bg-white flex justify-between items-center shrink-0">
                                     <div className="flex items-center gap-4">
                                         <div className="w-10 h-10 rounded-xl bg-indigo-50 border border-indigo-100 flex items-center justify-center shadow-inner"><i className="fas fa-table text-indigo-600 text-lg"></i></div>
                                         <div>
                                            <h2 className="font-bold text-gray-800 text-base leading-tight">{selTable}</h2>
                                            <div className="flex items-center gap-3 mt-1">
                                                 <p className="text-[10px] text-gray-400 font-mono uppercase tracking-widest font-semibold bg-gray-50 px-2 py-0.5 rounded border border-gray-100">{selDb}</p>
                                                 {schemaData?.lastUpdated && <span className="text-[10px] text-emerald-600 font-bold flex items-center gap-1.5 bg-emerald-50 px-2 py-0.5 rounded border border-emerald-100"><i className="fas fa-clock"></i> Updated: {schemaData.lastUpdated}</span>}
                                            </div>
                                         </div>
                                     </div>
                                     {!loading.schema && schemaData.rows.length > 0 && <div className="relative group"><i className="fas fa-filter absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 text-xs"></i><input type="text" placeholder="Filter columns..." value={colSearch} onChange={(e) => setColSearch(e.target.value)} className="w-56 bg-gray-50 border border-gray-200 text-gray-700 text-sm font-medium rounded-lg pl-8 pr-3 py-2 focus:outline-none focus:bg-white focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100 transition-all shadow-sm" /></div>}
                                  </div>
                                  <div className="flex-1 overflow-auto bg-white custom-scrollbar">
                                    {loading.schema ? <div className="py-24 flex flex-col items-center justify-center text-indigo-500 fade-in"><i className="fas fa-circle-notch fa-spin text-4xl mb-4"></i><p className="font-bold tracking-wide text-gray-600 text-sm">Fetching Schema...</p></div> : schemaData.rows.length > 0 && <div className="fade-in h-full pb-10"><GenericTable data={filteredSchema} isSearchActive={colSearch.length > 0} editMeta={editMeta} metaForm={metaForm} setMetaForm={setMetaForm} tableMeta={tableMeta} /></div>}
                                  </div>
                              </div>
                          </div>
                      ) : <div className="flex-1 flex flex-col items-center justify-center text-gray-400 text-center fade-in py-24 px-8"><div className="w-20 h-20 bg-white rounded-full flex items-center justify-center mb-4 border border-gray-200 shadow-sm"><i className="fas fa-table text-3xl text-gray-300"></i></div><p className="font-bold text-gray-600 text-lg mb-2">No Table Selected</p><p className="text-sm max-w-sm text-gray-500">Select a table from the explorer on the left to view its schema and documentation.</p></div>}
                  </div>
              )}

              {mainTab === 'preview' && (
                  <div className="flex-1 flex flex-col overflow-hidden bg-gray-50/30 fade-in">
                      <div className="px-8 py-6 border-b border-gray-200 bg-white shrink-0 flex justify-between items-center">
                          <div><h2 className="text-xl font-bold text-gray-800 flex items-center gap-2"><i className="fas fa-search text-indigo-500"></i> Secure Data Preview</h2><p className="text-sm text-gray-500 mt-1 font-medium">Viewing top 50 records for <span className="font-bold text-indigo-600 font-mono bg-indigo-50 px-2 py-0.5 rounded">{selTable}</span></p></div>
                          <button onClick={() => { setPreviewData(null); fetchPreview(); }} className="text-xs bg-white border border-gray-200 hover:border-indigo-300 text-gray-600 hover:text-indigo-600 px-3 py-1.5 rounded shadow-sm flex items-center gap-2 transition-all font-bold"><i className={`fas fa-sync-alt ${loading.preview ? 'fa-spin text-indigo-500' : ''}`}></i> Refresh</button>
                      </div>
                      <div className="flex-1 overflow-auto p-6">
                          {loading.preview ? <div className="py-24 flex flex-col items-center justify-center text-indigo-500 fade-in"><i className="fas fa-circle-notch fa-spin text-4xl mb-4"></i><p className="font-bold tracking-wide text-gray-600 text-sm">Fetching Secure Data Sample...</p></div> : previewData && previewData.length > 0 ? (
                              <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden"><div className="overflow-x-auto custom-scrollbar"><table className="min-w-full divide-y divide-gray-200"><thead className="bg-gray-50 sticky top-0 shadow-sm z-10"><tr>{Object.keys(previewData[0]).map(col => <th key={col} className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap">{col}</th>)}</tr></thead><tbody className="bg-white divide-y divide-gray-100">{previewData.map((row, i) => <tr key={i} className="hover:bg-indigo-50/40 transition-colors">{Object.values(row).map((val, j) => <td key={j} className="px-5 py-2 whitespace-nowrap text-xs text-gray-600 font-mono">{val === null ? <span className="text-gray-400 italic">NULL</span> : typeof val === 'object' ? JSON.stringify(val) : String(val)}</td>)}</tr>)}</tbody></table></div></div>
                          ) : <div className="py-24 text-center text-gray-400 italic">No data available for preview in this table.</div>}
                      </div>
                  </div>
              )}

              {/* PHASE 2 ADDITION: Real Time Table Profiling & Hudi Config */}
              {mainTab === 'profiling' && (
                  <div className="flex-1 flex flex-col overflow-hidden bg-gray-50/30 fade-in">
                      <div className="px-8 py-6 border-b border-gray-200 bg-white shrink-0 flex justify-between items-center">
                          <div><h2 className="text-xl font-bold text-gray-800 flex items-center gap-2"><i className="fas fa-search-plus text-indigo-500"></i> Column Profiling & Table Health</h2><p className="text-sm text-gray-500 mt-1 font-medium">Analyze distinct counts, null ratios, and data ranges for <span className="font-bold text-indigo-600 font-mono bg-indigo-50 px-2 py-0.5 rounded">{selTable}</span></p></div>
                          <button onClick={() => setColumnProfiles({})} className="text-xs bg-white border border-gray-200 hover:border-indigo-300 text-gray-600 hover:text-indigo-600 px-3 py-1.5 rounded shadow-sm flex items-center gap-2 transition-all font-bold"><i className="fas fa-eraser"></i> Clear Results</button>
                      </div>
                      
                      <div className="flex-1 overflow-auto p-6">
                          
                          {/* NEW: Table Health & Hudi Config Panel */}
                          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 mb-6">
                             <h3 className="font-bold text-gray-800 mb-4"><i className="fas fa-stethoscope text-emerald-500 mr-2"></i> Table Properties & Configuration</h3>
                             {loading.details ? (
                                 <div className="flex items-center text-indigo-500 text-sm"><i className="fas fa-circle-notch fa-spin mr-2"></i> Scanning Engine Metadata...</div>
                             ) : (
                               <div className="grid grid-cols-5 gap-4">
                                   <div className="p-4 bg-gray-50 rounded-lg border border-gray-100 flex flex-col justify-center">
                                       <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Live Row Count</p>
                                       <p className="text-xl font-black text-gray-800">{tableDetails.totalRows !== null ? tableDetails.totalRows.toLocaleString() : 'N/A'}</p>
                                   </div>
                                   <div className="p-4 bg-gray-50 rounded-lg border border-gray-100 flex flex-col justify-center">
                                       <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider mb-1">Table Size</p>
                                       <p className="text-xl font-black text-gray-800">{tableDetails.tableSize || 'N/A'}</p>
                                       {tableDetails.numFiles > 0 && <p className="text-xs text-gray-400 mt-1">{tableDetails.numFiles.toLocaleString()} parquet files</p>}
                                   </div>
                                   
                                   {tableDetails.hudiConfig?.isHudi ? (
                                     <>
                                       <div className="p-4 bg-indigo-50 rounded-lg border border-indigo-100">
                                           <p className="text-[10px] font-bold text-indigo-600 uppercase tracking-wider mb-1">Hudi Table Type</p>
                                           <p className="text-sm font-bold text-indigo-800">{tableDetails.hudiConfig.tableType}</p>
                                       </div>
                                       <div className="p-4 bg-indigo-50 rounded-lg border border-indigo-100">
                                           <p className="text-[10px] font-bold text-indigo-600 uppercase tracking-wider mb-1">Record Key(s)</p>
                                           <p className="text-xs font-mono text-indigo-800 break-all">{tableDetails.hudiConfig.recordKeys}</p>
                                       </div>
                                       <div className="p-4 bg-indigo-50 rounded-lg border border-indigo-100">
                                           <p className="text-[10px] font-bold text-indigo-600 uppercase tracking-wider mb-1">Partition / Precombine</p>
                                           <p className="text-xs font-mono text-indigo-800"><span className="text-[10px] uppercase text-indigo-500">PComb:</span> {tableDetails.hudiConfig.precombineKey}<br/><span className="text-[10px] uppercase text-indigo-500">Part:</span> {tableDetails.hudiConfig.partitionFields}</p>
                                       </div>
                                     </>
                                   ) : (
                                       <div className="col-span-3 p-4 bg-yellow-50 rounded-lg border border-yellow-100 flex items-center">
                                           <i className="fas fa-info-circle text-yellow-500 mr-3 text-xl"></i>
                                           <div><p className="text-sm font-bold text-yellow-800">Standard OLAP Table</p><p className="text-xs text-yellow-700">No Hudi configuration properties found for this specific asset.</p></div>
                                       </div>
                                   )}
                               </div>
                             )}
                          </div>

                          {/* Existing Column Profiling Table */}
                          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden"><div className="overflow-x-auto custom-scrollbar"><table className="min-w-full divide-y divide-gray-200">
                                      <thead className="bg-gray-50 sticky top-0 shadow-sm z-10"><tr><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Column Name</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Data Type</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Null %</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Distinct Count</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Min Value</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Max Value</th><th className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider w-48">Value Distribution</th><th className="px-5 py-3 text-right text-[11px] font-bold text-gray-500 uppercase tracking-wider">Action</th></tr></thead>
                                      <tbody className="bg-white divide-y divide-gray-100">{filteredSchema.map((row, i) => {
                                          const fieldName = row.Field || row.col_name || row.COLUMN_NAME || Object.values(row)[0];
                                          const dataType = row.Type || row.data_type || row.DATA_TYPE || '';
                                          const profile = columnProfiles[fieldName];
                                          const isProfiling = profilingCol === fieldName;
                                          return (
                                              <tr key={i} className="hover:bg-indigo-50/40 transition-colors">
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] font-bold text-gray-700">{fieldName}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap"><span className={`px-2 py-0.5 rounded text-[10px] font-semibold tracking-wide border shadow-sm ${getTypeStyle(dataType)}`}>{dataType.length > 20 ? dataType.substring(0, 17) + '...' : dataType}</span></td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] text-gray-600 font-mono">{profile ? `${profile.total_rows > 0 ? ((profile.null_count / profile.total_rows) * 100).toFixed(1) : 0}%` : '-'}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] text-gray-600 font-mono">{profile ? Number(profile.distinct_count).toLocaleString() : '-'}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] text-gray-600 font-mono max-w-[150px] truncate" title={profile ? profile.min_val : ''}>{profile ? (profile.min_val !== null ? String(profile.min_val) : 'N/A') : '-'}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-[13px] text-gray-600 font-mono max-w-[150px] truncate" title={profile ? profile.max_val : ''}>{profile ? (profile.max_val !== null ? String(profile.max_val) : 'N/A') : '-'}</td>
                                                  <td className="px-5 py-2 text-[13px] text-gray-600 font-mono w-48">{profile && profile.top_values && profile.top_values.length > 0 ? (<div className="flex flex-col gap-1 w-full">{profile.top_values.map((tv, vIdx) => { const pct = profile.total_rows > 0 ? ((tv.count / profile.total_rows) * 100).toFixed(1) : 0; return (<div key={vIdx} className="flex justify-between items-center text-[10px] bg-gray-50 px-2 py-0.5 rounded border border-gray-100 relative overflow-hidden group"><div className="absolute left-0 top-0 bottom-0 bg-indigo-100/50 z-0 transition-all" style={{ width: `${pct}%` }}></div><span className="truncate max-w-[100px] z-10 font-medium" title={tv.value}>{tv.value === '' ? '(Empty)' : tv.value}</span><span className="text-gray-500 z-10 text-[9px]">{pct}%</span></div>);})}</div>) : (profile ? <span className="text-gray-400 italic text-xs">No data</span> : '-')}</td>
                                                  <td className="px-5 py-3 whitespace-nowrap text-right"><button onClick={() => profileColumn(fieldName)} disabled={isProfiling} className={`text-[10px] font-bold uppercase tracking-wider px-3 py-1.5 rounded transition-all shadow-sm border ${isProfiling ? 'bg-gray-100 text-gray-400 border-gray-200' : 'bg-white text-indigo-600 border-indigo-200 hover:border-indigo-400 hover:shadow-md'}`}>{isProfiling ? <><i className="fas fa-spinner fa-spin mr-1"></i> Profiling</> : <><i className="fas fa-chart-pie mr-1"></i> Profile</>}</button></td>
                                              </tr>
                                          );
                                      })}
                                      </tbody>
                                  </table></div></div>
                      </div>
                  </div>
              )}

              {mainTab === 'observability' && (
                  <div className="flex-1 flex flex-col overflow-y-auto custom-scrollbar p-8 bg-[#fdfdfd] fade-in">
                      <div className="flex justify-between items-center mb-8 shrink-0">
                          <div><h2 className="text-xl font-bold text-gray-800">Volume Profiler</h2><p className="text-sm text-gray-500 mt-1 font-medium">Track row ingestion and active records for <span className="font-bold text-indigo-600 font-mono bg-indigo-50 px-2 py-0.5 rounded">{selTable}</span></p></div>
                          <button onClick={runProfiler} disabled={loading.obs} className="bg-indigo-600 text-white hover:bg-indigo-700 px-5 py-2.5 rounded-lg shadow-md font-bold flex items-center gap-2 hover:-translate-y-0.5 transition-all duration-300 disabled:opacity-50 disabled:transform-none">{loading.obs ? <i className="fas fa-circle-notch fa-spin"></i> : <i className="fas fa-play"></i>}{loading.obs ? 'Scanning Lake...' : 'Run Calculation'}</button>
                      </div>

                      {renderObservabilityCards()}
                      {renderTrendChart()}

                      {obsHistory.length === 0 && !loading.obs && (
                          <div className="bg-white rounded-2xl border border-gray-200 border-dashed p-12 text-center mb-8 shadow-sm shrink-0">
                              <div className="w-16 h-16 bg-indigo-50 rounded-full flex items-center justify-center mb-4 mx-auto"><i className="fas fa-chart-bar text-indigo-400 text-2xl"></i></div>
                              <h3 className="text-lg font-bold text-gray-700 mb-1">No Profiling History</h3><p className="text-sm text-gray-500">Run a new calculation to establish a baseline for this table.</p>
                          </div>
                      )}

                      {obsHistory.length > 0 && (
                          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col shrink-0 mb-8">
                              <div className="p-4 border-b border-gray-100 bg-gray-50 flex justify-between items-center shrink-0">
                                  <h3 className="text-[11px] font-bold text-gray-500 uppercase tracking-widest flex items-center gap-2"><i className="fas fa-history text-gray-400"></i> Historical Snapshots</h3>
                                  <span className="text-[9px] bg-emerald-50 text-emerald-700 border border-emerald-100 px-2 py-1 rounded font-bold tracking-widest flex items-center gap-1 shadow-sm uppercase"><i className="fas fa-bolt text-emerald-500"></i> StarRocks Profiler</span>
                              </div>
                              <div className="overflow-x-auto">
                                  <table className="min-w-full divide-y divide-gray-100">
                                    <thead className="bg-white sticky top-0"><tr><th className="px-6 py-4 text-left text-[11px] font-bold text-gray-400 uppercase tracking-wider">Date Logged</th><th className="px-6 py-4 text-left text-[11px] font-bold text-indigo-500 uppercase tracking-wider">Target Date</th><th className="px-6 py-4 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider">Total Rows</th><th className="px-6 py-4 text-left text-[11px] font-bold text-emerald-600 uppercase tracking-wider">New Incremental</th></tr></thead>
                                    <tbody className="bg-white divide-y divide-gray-50">
                                      {obsHistory.map((run, i) => {
                                          const measureDate = new Date(run.measured_at);
                                          return (
                                            <tr key={i} className="hover:bg-indigo-50/30 transition-colors group">
                                              <td className="px-6 py-3 whitespace-nowrap text-sm text-gray-500 font-mono font-medium group-hover:text-indigo-600 transition-colors">{measureDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })} <span className="text-gray-400 text-xs mx-1">at</span> {measureDate.toLocaleTimeString('en-US', { hour: '2-digit', minute:'2-digit' })}</td>
                                              <td className="px-6 py-3 whitespace-nowrap text-sm font-bold text-indigo-600 bg-indigo-50/30">{run.target_date || '-'}</td>
                                              <td className="px-6 py-3 whitespace-nowrap text-sm font-bold text-gray-700">{Number(run.total_count).toLocaleString()}</td>
                                              <td className="px-6 py-3 whitespace-nowrap text-sm font-bold text-emerald-600">{run.today_count !== null ? (<span className="bg-emerald-50 px-2 py-0.5 rounded border border-emerald-100">{Number(run.today_count) > 0 ? `+${Number(run.today_count).toLocaleString()}` : Number(run.today_count).toLocaleString()}</span>) : '-'}</td>
                                            </tr>
                                          );
                                      })}
                                    </tbody>
                                  </table>
                              </div>
                          </div>
                      )}
                  </div>
              )}

              {mainTab === 'workbench' && (
                  <div className={`flex-1 flex flex-col overflow-hidden bg-gray-50/30 fade-in ${isDragging ? 'select-none cursor-row-resize' : ''}`}>
                      <div className="px-8 py-6 border-b border-gray-200 bg-white shrink-0 flex justify-between items-center">
                          <div><h2 className="text-xl font-bold text-gray-800 flex items-center gap-2"><i className="fas fa-terminal text-indigo-500"></i> Ad-Hoc SQL Workbench</h2><p className="text-sm text-gray-500 mt-1 font-medium">Run read-only queries against <span className="font-bold text-indigo-600 font-mono bg-indigo-50 px-2 py-0.5 rounded">{selDb || selConn.name}</span></p></div>
                          <button onClick={executeWorkbenchQuery} disabled={loading.workbench || !workbenchQuery.trim()} className="bg-indigo-600 text-white hover:bg-indigo-700 px-5 py-2.5 rounded-lg shadow-md font-bold flex items-center gap-2 hover:-translate-y-0.5 transition-all duration-300 disabled:opacity-50 disabled:transform-none">{loading.workbench ? <i className="fas fa-circle-notch fa-spin"></i> : <i className="fas fa-play"></i>} {loading.workbench ? 'Running...' : 'Run Query'}</button>
                      </div>
                      
                      <div className="px-6 pt-6 pb-4 shrink-0 bg-white">
                          <div 
                              className="flex w-full bg-[#1e1e1e] rounded-xl focus-within:ring-2 focus-within:ring-indigo-500 shadow-inner overflow-hidden border border-gray-800"
                              style={{ height: `${editorHeight}px` }}
                          >
                              {/* Line Numbers Gutter */}
                              <div 
                                  ref={lineNumbersRef}
                                  className="w-12 bg-[#252526] text-gray-500 font-mono text-sm text-right py-4 pr-3 select-none overflow-hidden leading-6"
                              >
                                  {workbenchQuery.split('\n').map((_, i) => <div key={i}>{i + 1}</div>)}
                              </div>
                              {/* SQL Input Area */}
                              <textarea 
                                  value={workbenchQuery} 
                                  onChange={(e) => setWorkbenchQuery(e.target.value)}
                                  onScroll={(e) => { if (lineNumbersRef.current) lineNumbersRef.current.scrollTop = e.target.scrollTop; }}
                                  className="flex-1 bg-transparent text-[#9cdcfe] font-mono p-4 focus:outline-none text-sm resize-none whitespace-pre overflow-auto leading-6 custom-scrollbar"
                                  wrap="off"
                                  spellCheck="false"
                                  placeholder="SELECT * FROM table_name LIMIT 10"
                              />
                          </div>
                          <p className="text-[10px] text-gray-400 mt-2 flex items-center gap-1.5 font-semibold"><i className="fas fa-shield-alt text-emerald-500"></i> Read-only mode active. Results are masked and strictly limited to 100 rows.</p>
                      </div>
                      
                      {/* Custom Horizontal Draggable Splitter */}
                      <div 
                          onMouseDown={startDrag}
                          className="h-2 bg-gray-200 hover:bg-indigo-400 cursor-row-resize w-full transition-colors flex items-center justify-center group z-10 border-y border-gray-300/50"
                          title="Drag to resize editor"
                      >
                          <div className="w-12 h-0.5 bg-gray-400 group-hover:bg-white rounded-full"></div>
                      </div>

                      <div className="flex-1 overflow-auto p-6 bg-gray-50">
                          {loading.workbench && <div className="py-12 flex flex-col items-center justify-center text-indigo-500"><i className="fas fa-circle-notch fa-spin text-3xl mb-4"></i><p className="font-bold">Executing Query...</p></div>}
                          {workbenchError && (
                              <div className="bg-rose-50 border border-rose-200 text-rose-700 p-4 rounded-xl shadow-sm flex items-start gap-3">
                                  <i className="fas fa-exclamation-circle mt-1"></i>
                                  <div><h4 className="font-bold text-sm mb-1">Query Failed</h4><p className="text-xs font-mono whitespace-pre-wrap">{workbenchError}</p></div>
                              </div>
                          )}
                          {!loading.workbench && !workbenchError && workbenchResults && (
                              workbenchResults.length > 0 ? (
                                  <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden"><div className="overflow-x-auto custom-scrollbar"><table className="min-w-full divide-y divide-gray-200"><thead className="bg-gray-50 sticky top-0 shadow-sm z-10"><tr>{Object.keys(workbenchResults[0]).map(col => <th key={col} className="px-5 py-3 text-left text-[11px] font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap">{col}</th>)}</tr></thead><tbody className="bg-white divide-y divide-gray-100">{workbenchResults.map((row, i) => <tr key={i} className="hover:bg-indigo-50/40 transition-colors">{Object.values(row).map((val, j) => <td key={j} className="px-5 py-2 whitespace-nowrap text-xs text-gray-600 font-mono">{val === null ? <span className="text-gray-400 italic">NULL</span> : typeof val === 'object' ? JSON.stringify(val) : String(val)}</td>)}</tr>)}</tbody></table></div></div>
                              ) : <div className="py-12 text-center text-gray-500 italic bg-white rounded-xl border border-gray-200 shadow-sm">Query executed successfully, but returned 0 rows.</div>
                          )}
                      </div>
                  </div>
              )}
            </div>
        </div>
      </div>
  );
}
