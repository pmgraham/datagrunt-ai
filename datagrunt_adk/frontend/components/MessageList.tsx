import React, { useRef, useEffect } from 'react';
import { ChatMessage } from '../types';
import { ChatBubble } from './ChatBubble';
import { Upload } from 'lucide-react';

interface MessageListProps {
  messages: ChatMessage[];
  onOpenCanvas?: () => void;
  uploadProgress?: number | null;
}

export const MessageList: React.FC<MessageListProps> = ({ messages, onOpenCanvas, uploadProgress }) => {
  const bottomRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, messages[messages.length - 1]?.text, uploadProgress]);

  return (
    <div ref={containerRef} className="flex-1 overflow-y-auto px-4 py-6">
      <div className="max-w-5xl mx-auto">
        {messages.map((msg) => (
          <ChatBubble key={msg.id} message={msg} onOpenCanvas={onOpenCanvas} />
        ))}

        {uploadProgress != null && (
          <div className="flex justify-start mb-4">
            <div className="flex gap-3 max-w-[85%] md:max-w-[75%]">
              <div className="flex-shrink-0 mt-1">
                <div className="w-7 h-7 bg-gradient-to-tr from-primary-600 to-indigo-600 rounded-full flex items-center justify-center">
                  <Upload className="w-3.5 h-3.5 text-white" />
                </div>
              </div>
              <div className="min-w-0 flex-1">
                <div className="bg-white rounded-2xl rounded-tl-md border border-slate-200 shadow-sm px-4 py-3">
                  <div className="flex items-center gap-3">
                    <p className="text-sm text-slate-600">
                      Uploading file{uploadProgress < 100 ? '...' : ' — processing'}
                    </p>
                    <span className="text-xs font-medium text-slate-400">{uploadProgress}%</span>
                  </div>
                  <div className="mt-2 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-primary-500 rounded-full transition-all duration-200 ease-out"
                      style={{ width: `${uploadProgress}%` }}
                    />
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        <div ref={bottomRef} />
      </div>
    </div>
  );
};
