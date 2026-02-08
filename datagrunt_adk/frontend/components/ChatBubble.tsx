import React from 'react';
import { ChatMessage } from '../types';
import { MarkdownReport } from './MarkdownReport';
import { ToolStatusBar } from './ToolStatusBar';
import { getDownloadUrl } from '../services/adkService';
import { Download, Paperclip, Sparkles, Maximize2, FileCheck } from 'lucide-react';
interface ChatBubbleProps {
  message: ChatMessage;
  onOpenCanvas?: () => void;
  isCanvasReport?: boolean;
}

function splitSafeMarkdown(text: string): { safe: string; tail: string } {
  const lastBreak = text.lastIndexOf('\n\n');
  if (lastBreak === -1) return { safe: '', tail: text };
  return { safe: text.slice(0, lastBreak), tail: text.slice(lastBreak + 2) };
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export const ChatBubble: React.FC<ChatBubbleProps> = ({ message, onOpenCanvas, isCanvasReport }) => {
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
  // Streaming messages → show streaming text inline
  const isFinalized = !message.isStreaming && !!message.text;
  const showAsCard = isFinalized && !!isCanvasReport;

  // For streaming messages, split markdown safely for partial rendering
  const { safe, tail } = message.isStreaming
    ? splitSafeMarkdown(message.text)
    : { safe: '', tail: '' };

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
            </div>
          ) : isFinalized ? (
            /* Short conversational reply — render inline as markdown */
            <div className="bg-white rounded-2xl rounded-tl-md border border-slate-200 shadow-sm px-4 py-3 overflow-hidden">
              <MarkdownReport content={message.text} bare />
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
          ) : (message.text || !message.isStreaming) ? (
            /* Streaming bubble — show text as it arrives */
            <div className="bg-white rounded-2xl rounded-tl-md border border-slate-200 shadow-sm px-4 py-3">
              {safe && <MarkdownReport content={safe} bare />}

              {tail && (
                <p className="text-slate-600 text-sm leading-relaxed whitespace-pre-wrap">
                  {tail}
                  {message.isStreaming && (
                    <span className="inline-block w-1.5 h-4 bg-primary-500 ml-0.5 animate-pulse rounded-sm align-text-bottom" />
                  )}
                </p>
              )}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
};
