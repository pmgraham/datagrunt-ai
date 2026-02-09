import React from 'react';
import { ChatMessage } from '../types';
import { MarkdownReport } from './MarkdownReport';
import { ToolStatusBar } from './ToolStatusBar';
import { getDownloadUrl } from '../services/adkService';
import { deduplicateReport } from '../utils';
import { Download, Paperclip, Sparkles, Maximize2, FileCheck, CheckCircle, Edit3 } from 'lucide-react';

interface ChatBubbleProps {
  message: ChatMessage;
  onOpenCanvas?: () => void;
  isCanvasReport?: boolean;
  onSendMessage?: (text: string) => void;
  showActions?: boolean;
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export const ChatBubble: React.FC<ChatBubbleProps> = ({ message, onOpenCanvas, isCanvasReport, onSendMessage, showActions }) => {
  if (message.role === 'user') {
    return (
      <div className="flex justify-end mb-4">
        <div className="max-w-[80%] md:max-w-[60%]">
          {message.fileAttachment ? (
            <div className="bg-primary-600 text-white rounded-2xl rounded-br-md px-4 py-3">
              <div className="flex items-center gap-2">
                <Paperclip className="w-4 h-4 flex-shrink-0" />
                <div className="min-w-0">
                  <p className="font-medium text-sm truncate">{message.fileAttachment.name}</p>
                  <p className="text-xs text-primary-200">{formatFileSize(message.fileAttachment.size)}</p>
                </div>
              </div>
              {message.text && (
                <p className="mt-2 text-sm">{message.text}</p>
              )}
            </div>
          ) : (
            <div className="bg-primary-600 text-white rounded-2xl rounded-br-md px-4 py-3">
              <p className="text-sm whitespace-pre-wrap">{message.text}</p>
            </div>
          )}
        </div>
      </div>
    );
  }

  // Finalized agent messages with text → compact card (only the canvas report) OR inline markdown
  // Streaming messages → show "generating" indicator (no raw markdown)
  const isFinalized = !message.isStreaming && !!message.text;
  const showAsCard = isFinalized && !!isCanvasReport;

  return (
    <div className="flex justify-start mb-4">
      <div className="flex gap-3 max-w-[85%] md:max-w-[75%]">
        <div className="flex-shrink-0 mt-1">
          <div className="w-7 h-7 bg-gradient-to-tr from-primary-600 to-indigo-600 rounded-full flex items-center justify-center">
            <Sparkles className="w-3.5 h-3.5 text-white" />
          </div>
        </div>

        <div className="min-w-0 flex-1">
          <ToolStatusBar toolCalls={message.toolCalls} isStreaming={message.isStreaming} />

          {showAsCard ? (
            /* Compact card — report content lives in the canvas */
            <div className="bg-white rounded-2xl rounded-tl-md border border-slate-200 shadow-sm px-4 py-3">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-emerald-50 rounded-lg flex-shrink-0">
                  <FileCheck className="w-4 h-4 text-emerald-600" />
                </div>
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-medium text-slate-800">
                    {message.downloadableFile ? 'Analysis complete' : 'Report ready'}
                  </p>
                  <p className="text-xs text-slate-500">
                    {message.downloadableFile ? 'Report and cleaned data ready' : 'View details in canvas'}
                  </p>
                </div>
              </div>

              <div className="mt-3 pt-3 border-t border-slate-100 flex items-center gap-3">
                {onOpenCanvas && (
                  <button
                    onClick={onOpenCanvas}
                    className="inline-flex items-center gap-1.5 text-xs font-medium text-primary-600 hover:text-primary-700 transition-colors"
                  >
                    <Maximize2 className="w-3.5 h-3.5" />
                    View Report
                  </button>
                )}
                {message.downloadableFile && (
                  <a
                    href={getDownloadUrl(message.downloadableFile.path)}
                    className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-500 hover:text-slate-700 transition-colors"
                    download
                  >
                    <Download className="w-3.5 h-3.5" />
                    Download {message.downloadableFile.name}
                  </a>
                )}
              </div>

              {showActions && onSendMessage && (
                <div className="mt-3 pt-3 border-t border-slate-100 flex items-center gap-3">
                  <button
                    onClick={() => onSendMessage('Yes, apply the cleaning plan.')}
                    className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors"
                  >
                    <CheckCircle className="w-4 h-4" />
                    Accept Plan
                  </button>
                  <button
                    onClick={() => onSendMessage('I would like to make some changes to the plan before applying it.')}
                    className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-amber-800 bg-amber-100 hover:bg-amber-200 rounded-lg transition-colors"
                  >
                    <Edit3 className="w-4 h-4" />
                    Make Changes
                  </button>
                </div>
              )}
            </div>
          ) : isFinalized ? (
            /* Short conversational reply — render inline as markdown */
            <div className="bg-white rounded-2xl rounded-tl-md border border-slate-200 shadow-sm px-4 py-3 overflow-hidden">
              <MarkdownReport content={deduplicateReport(message.text)} bare />
              {message.downloadableFile && (
                <div className="mt-3 pt-3 border-t border-slate-100">
                  <a
                    href={getDownloadUrl(message.downloadableFile.path)}
                    className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-500 hover:text-slate-700 transition-colors"
                    download
                  >
                    <Download className="w-3.5 h-3.5" />
                    Download {message.downloadableFile.name}
                  </a>
                </div>
              )}
            </div>
          ) : message.isStreaming ? (
            /* Streaming — show generating indicator, hide raw markdown */
            <div className="bg-white rounded-2xl rounded-tl-md border border-slate-200 shadow-sm px-4 py-3">
              <div className="flex items-center gap-2 text-slate-500">
                <div className="flex gap-1">
                  <span className="w-1.5 h-1.5 bg-primary-500 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                  <span className="w-1.5 h-1.5 bg-primary-500 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                  <span className="w-1.5 h-1.5 bg-primary-500 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
                </div>
                <span className="text-sm">Generating response...</span>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
};
