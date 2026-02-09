import React from 'react';
import { Sparkles, Github, RotateCcw } from 'lucide-react';

interface HeaderProps {
  onReset?: () => void;
}

export const Header: React.FC<HeaderProps> = ({ onReset }) => {
  return (
    <header className="bg-white border-b border-slate-200 sticky top-0 z-50 flex-shrink-0">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-14 flex items-center justify-between">
        <div className="flex items-center space-x-2">
          <div className="bg-gradient-to-tr from-primary-600 to-indigo-600 p-1.5 rounded-lg text-white">
            <Sparkles className="w-4 h-4" />
          </div>
          <span className="text-lg font-bold bg-clip-text text-transparent bg-gradient-to-r from-slate-900 to-slate-700">
            DataGrunt AI
          </span>
        </div>
        <div className="flex items-center gap-3">
          {onReset && (
            <button
              onClick={onReset}
              className="flex items-center gap-1.5 text-xs font-medium text-slate-500 hover:text-slate-700 transition-colors px-2 py-1 rounded-md hover:bg-slate-100"
            >
              <RotateCcw className="w-3.5 h-3.5" />
              New Conversation
            </button>
          )}
          <span className="hidden sm:inline-block text-xs text-slate-400">Powered by Gemini</span>
          <a href="#" className="text-slate-400 hover:text-slate-600 transition-colors">
            <Github className="w-4 h-4" />
          </a>
        </div>
      </div>
    </header>
  );
};
