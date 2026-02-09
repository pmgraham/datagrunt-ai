import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { ChatMessage, CleanedDataRow } from '../types';
import { MarkdownReport } from './MarkdownReport';
import { DataTableModal } from './DataTableModal';
import { fetchPreviewRows } from '../services/adkService';
import { getDownloadUrl } from '../services/adkService';
import { X, Download, FileDown, Table as TableIcon } from 'lucide-react';
import { isReportMessage, deduplicateReport } from '../utils';

interface CanvasProps {
  messages: ChatMessage[];
  onClose: () => void;
  onSendMessage: (text: string) => void;
  isAgentRunning: boolean;
}

export const Canvas: React.FC<CanvasProps> = ({
  messages,
  onClose,
  onSendMessage,
  isAgentRunning,
}) => {
  const [previewData, setPreviewData] = useState<CleanedDataRow[] | null>(null);
  const [totalRows, setTotalRows] = useState(0);
  const [loadingPreview, setLoadingPreview] = useState(false);
  const [dataLoaded, setDataLoaded] = useState(false);
  const [isDataModalOpen, setIsDataModalOpen] = useState(false);

  // Always show the most detailed report in the canvas. Pick the longest
  // report-length agent message so that short cleaning confirmations or
  // follow-up replies never replace the initial analysis.
  // Also deduplicate if the model repeated the report.
  const reportContent = useMemo(() => {
    let best = '';
    for (const m of messages) {
      if (m.role === 'agent' && !m.isStreaming && m.text && isReportMessage(m.text)) {
        if (m.text.length > best.length) {
          best = m.text;
        }
      }
    }
    return deduplicateReport(best);
  }, [messages]);

  // Check if any message has a downloadable file (Data tab available)
  const hasData = useMemo(() => {
    return messages.some((m) => !!m.downloadableFile);
  }, [messages]);

  const loadData = useCallback(async (force = false) => {
    if ((!force && dataLoaded) || loadingPreview) return;

    setLoadingPreview(true);
    try {
      const result = await fetchPreviewRows('data', 100, 0);
      setPreviewData(result.rows);
      setTotalRows(result.total);
      setDataLoaded(true);
    } finally {
      setLoadingPreview(false);
    }
  }, [dataLoaded, loadingPreview]);

  const refreshData = useCallback(() => {
    loadData(true);
  }, [loadData]);

  useEffect(() => {
    if (isDataModalOpen && !dataLoaded) {
      loadData();
    }
  }, [isDataModalOpen, dataLoaded, loadData]);

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
          {hasData && (
            <button
              onClick={() => setIsDataModalOpen(true)}
              className="p-1 rounded-md text-slate-400 hover:text-slate-600 hover:bg-slate-200 transition-colors"
              aria-label="View data"
              title="View data"
            >
              <TableIcon className="w-4 h-4" />
            </button>
          )}
          {reportContent && (
            <button
              onClick={() => {
                const blob = new Blob([reportContent], { type: 'text/markdown;charset=utf-8' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'analysis_report.md';
                a.click();
                URL.revokeObjectURL(url);
              }}
              className="p-1 rounded-md text-slate-400 hover:text-slate-600 hover:bg-slate-200 transition-colors"
              aria-label="Download report as Markdown"
              title="Download report as Markdown"
            >
              <FileDown className="w-4 h-4" />
            </button>
          )}
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

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6">
        {reportContent && <MarkdownReport content={reportContent} />}
      </div>

      {/* Data Modal */}
      <DataTableModal
        isOpen={isDataModalOpen}
        onClose={() => setIsDataModalOpen(false)}
        data={previewData}
        totalRows={totalRows}
        loading={loadingPreview}
        onSendMessage={onSendMessage}
        isAgentRunning={isAgentRunning}
        messages={messages}
        onRefreshData={refreshData}
      />
    </div>
  );
};
