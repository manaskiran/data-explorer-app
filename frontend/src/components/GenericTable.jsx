import React from 'react';
import { getTypeStyle } from '../utils/theme';

const GenericTable = ({ data, isSearchActive, editMeta, metaForm, setMetaForm, tableMeta }) => {
  if (!data || !Array.isArray(data) || data.length === 0) return <div className="p-12 text-center text-gray-500 italic text-sm">{isSearchActive ? 'No columns match your search.' : 'No schema data to display.'}</div>;
  
  const baseHeaders = Object.keys(data[0]).filter(h => h !== 'Comment' && h !== 'Description');
  const headers = [...baseHeaders, 'Description'];

  return (
    <div className="overflow-x-auto h-full rounded-b-xl">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50/90 sticky top-0 shadow-sm z-10 backdrop-blur-md">
          <tr>{headers.map(h => <th key={h} className="px-5 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wider whitespace-nowrap">{h}</th>)}</tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-100">
          {data.map((row, i) => {
            const fieldName = row.Field || row.col_name || row.COLUMN_NAME || Object.values(row)[0];
            const nativeComment = row.Comment || row.comment || '';
            const displayComment = tableMeta?.column_comments?.[fieldName] || nativeComment;
            const formComment = metaForm?.column_comments?.[fieldName] !== undefined ? metaForm.column_comments[fieldName] : displayComment;

            return (
              <tr key={i} className="hover:bg-indigo-50/30 transition-colors group">
                {baseHeaders.map((h, j) => {
                    const val = String(row[h]);
                    return (
                      <td key={j} className={`px-5 py-3 whitespace-nowrap text-[13px] ${j===0 ? 'font-normal text-gray-700' : 'text-gray-500'}`}>
                          {row[h] === null ? <span className="text-gray-400 italic">NULL</span> : 
                              h.toLowerCase().includes('type') ? <span className={`px-2 py-0.5 rounded text-[10px] font-semibold tracking-wide border shadow-sm ${getTypeStyle(val)}`}>{val.length > 20 ? val.substring(0, 17) + '...' : val}</span> : val 
                          }
                      </td>
                    );
                })}
                <td className="px-5 py-2 text-[13px] text-gray-600 min-w-[300px]">
                    {editMeta ? (
                        <input type="text" className="w-full bg-white border border-indigo-300 rounded px-3 py-1.5 text-[13px] focus:outline-none focus:border-indigo-500 focus:ring-2 focus:ring-indigo-100 shadow-inner transition-all placeholder-gray-300 text-gray-700" placeholder="Add business context..." value={formComment} onChange={(e) => setMetaForm(prev => ({ ...prev, column_comments: { ...(prev.column_comments || {}), [fieldName]: e.target.value } }))}/>
                    ) : <span className={`block w-full ${displayComment ? "text-gray-600" : "text-gray-400 italic transition-colors"}`}>{displayComment || 'No description provided.'}</span>}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  );
};
export default GenericTable;
