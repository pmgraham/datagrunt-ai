import React, { useState } from 'react';
import { CleanedDataRow } from '../types';
import { Download, Table as TableIcon, ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from './ui/Button';

interface DataTableProps {
  data: CleanedDataRow[];
  totalRows?: number;
}

export const DataTable: React.FC<DataTableProps> = ({ data, totalRows }) => {
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 25;

  if (!data || data.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-slate-400">
        <TableIcon className="w-12 h-12 mb-3 opacity-20" />
        <p>No data available to display</p>
      </div>
    );
  }

  const columns = Object.keys(data[0]);
  const totalPages = Math.ceil(data.length / rowsPerPage);
  
  const currentRows = data.slice(
    (currentPage - 1) * rowsPerPage,
    currentPage * rowsPerPage
  );

  const handleDownload = () => {
    // Simple CSV export
    const headers = columns.join(',');
    const rows = data.map(row => 
      columns.map(col => {
        const val = row[col];
        // Handle commas/quotes in data
        const str = String(val === null || val === undefined ? '' : val);
        return str.includes(',') || str.includes('"') || str.includes('\n') 
          ? `"${str.replace(/"/g, '""')}"` 
          : str;
      }).join(',')
    );
    const csvContent = [headers, ...rows].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', 'cleaned_data.csv');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center mb-4">
        <div className="flex items-center gap-2">
            <div className="p-2 bg-emerald-50 rounded-lg">
                <TableIcon className="w-5 h-5 text-emerald-600" />
            </div>
            <div>
                <h3 className="text-lg font-semibold text-slate-900">Cleaned Dataset</h3>
                <p className="text-sm text-slate-500">
                  {totalRows != null && totalRows > data.length
                    ? `Showing ${data.length} of ${totalRows.toLocaleString()} rows`
                    : `${data.length} rows`}
                </p>
            </div>
        </div>
        <Button onClick={handleDownload} variant="outline" size="sm" className="gap-2">
          <Download className="w-4 h-4" />
          Export CSV
        </Button>
      </div>

      <div className="overflow-hidden rounded-lg border border-slate-200 shadow-sm bg-white">
        <div className="overflow-x-auto scrollbar-thin">
          <table className="w-full text-left text-sm whitespace-nowrap">
            <thead className="bg-slate-50 border-b border-slate-200">
              <tr>
                {columns.map((col) => (
                  <th key={col} className="px-6 py-3 font-semibold text-slate-700">
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {currentRows.map((row, rowIndex) => (
                <tr 
                  key={rowIndex} 
                  className="hover:bg-slate-50/80 transition-colors"
                >
                  {columns.map((col) => (
                    <td key={`${rowIndex}-${col}`} className="px-6 py-3 text-slate-600">
                      {row[col]?.toString() || <span className="text-slate-300 italic">null</span>}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between py-3 border-t border-slate-200">
          <div className="text-sm text-slate-500">
            Page <span className="font-medium text-slate-900">{currentPage}</span> of <span className="font-medium text-slate-900">{totalPages}</span>
          </div>
          <div className="flex gap-2">
            <Button 
              variant="outline" 
              size="sm" 
              onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
              disabled={currentPage === 1}
            >
              <ChevronLeft className="w-4 h-4" />
            </Button>
            <Button 
              variant="outline" 
              size="sm" 
              onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
              disabled={currentPage === totalPages}
            >
              <ChevronRight className="w-4 h-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
};
