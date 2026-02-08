import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { ChatMessage, CleanedDataRow } from '../types';
import { MarkdownReport } from './MarkdownReport';
import { DataTable } from './DataTable';
import { fetchPreviewRows } from '../services/adkService';
import { getDownloadUrl } from '../services/adkService';
import { X, Download, FileText, Table as TableIcon } from 'lucide-react';
import { isReportMessage } from '../utils';

interface CanvasProps {
  messages: ChatMessage[];
  activeTab: 'report' | 'data';
  onTabChange: (tab: 'report' | 'data') => void;
  onClose: () => void;
}

export const Canvas: React.FC<CanvasProps> = ({
  messages,
  activeTab,
  onTabChange,
  onClose,
}) => {
  const [previewData, setPreviewData] = useState<CleanedDataRow[] | null>(null);
  const [totalRows, setTotalRows] = useState(0);
  const [loadingPreview, setLoadingPreview] = useState(false);
  const [dataLoaded, setDataLoaded] = useState(false);

  // Always show the most detailed report in the canvas. Pick the longest
  // report-length agent message so that short cleaning confirmations or
  // follow-up replies never replace the initial analysis.
  const reportContent = useMemo(() => {
    let best = '';
    for (const m of messages) {
      if (m.role === 'agent' && !m.isStreaming && m.text && isReportMessage(m.text)) {
        if (m.text.length > best.length) {
          best = m.text;
        }
      }
    }
    return best;
  }, [messages]);

  // Check if any message has a downloadable file (Data tab available)
  const hasData = useMemo(() => {
    return messages.some((m) => !!m.downloadableFile);
  }, [messages]);

  const loadData = useCallback(async () => {
    if (dataLoaded || loadingPreview) return;

    setLoadingPreview(true);
    const result = await fetchPreviewRows('data', 100, 0);
    setPreviewData(result.rows);
    setTotalRows(result.total);
    setDataLoaded(true);
    setLoadingPreview(false);
  }, [dataLoaded, loadingPreview]);

  useEffect(() => {
    if (activeTab === 'data' && !dataLoaded) {
      loadData();
    }
  }, [activeTab, dataLoaded, loadData]);

  // Reset cache when a new cleaned file appears
  const latestFilePath = useMemo(() => {
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].downloadableFile) return messages[i].downloadableFile!.path;
    }
    return null;
  }, [messages]);

  const [cachedFilePath, setCachedFilePath] = useState<string | null>(null);

  useEffect(() => {
    if (latestFilePath && latestFilePath !== cachedFilePath) {
      setCachedFilePath(latestFilePath);
      setPreviewData(null);
      setDataLoaded(false);
      setTotalRows(0);
    }
  }, [latestFilePath, cachedFilePath]);

  return (
    <div className="flex flex-col h-full bg-white border-l border-slate-200">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200 bg-slate-50/80">
        <h2 className="text-sm font-semibold text-slate-700">Analysis Report</h2>
        <div className="flex items-center gap-1">
          {latestFilePath && (
            <a
              href={getDownloadUrl(latestFilePath)}
              className="p-1 rounded-md text-slate-400 hover:text-slate-600 hover:bg-slate-200 transition-colors"
              aria-label="Download cleaned CSV"
              title="Download cleaned CSV"
              download
            >
              <Download className="w-4 h-4" />
            </a>
          )}
          <button
            onClick={onClose}
            className="p-1 rounded-md text-slate-400 hover:text-slate-600 hover:bg-slate-200 transition-colors"
            aria-label="Close canvas"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Tab bar */}
      <div className="flex border-b border-slate-200 bg-slate-50/40">
        <button
          onClick={() => onTabChange('report')}
          className={`flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium transition-colors border-b-2 ${
            activeTab === 'report'
              ? 'border-primary-600 text-primary-700'
              : 'border-transparent text-slate-500 hover:text-slate-700'
          }`}
        >
          <FileText className="w-3.5 h-3.5" />
          Report
        </button>
        {hasData && (
          <button
            onClick={() => onTabChange('data')}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium transition-colors border-b-2 ${
              activeTab === 'data'
                ? 'border-primary-600 text-primary-700'
                : 'border-transparent text-slate-500 hover:text-slate-700'
            }`}
          >
            <TableIcon className="w-3.5 h-3.5" />
            Data
          </button>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6">
        {activeTab === 'report' && reportContent && (
          <MarkdownReport content={reportContent} />
        )}

        {activeTab === 'data' && (
          <>
            {loadingPreview && (
              <div className="flex items-center justify-center py-12 text-slate-400">
                <div className="animate-spin w-5 h-5 border-2 border-slate-300 border-t-primary-600 rounded-full mr-3" />
                Loading data preview...
              </div>
            )}
            {!loadingPreview && previewData && (
              <DataTable data={previewData} totalRows={totalRows} />
            )}
          </>
        )}
      </div>
    </div>
  );
};
