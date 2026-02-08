import React, { useRef, useCallback, useState } from 'react';
import { UploadCloud, RefreshCw, CheckCircle, FileSpreadsheet } from 'lucide-react';
import { Button } from './ui/Button';

interface WelcomeScreenProps {
  onFileUpload: (file: File) => void;
  onSampleLoad: () => void;
}

export const WelcomeScreen: React.FC<WelcomeScreenProps> = ({ onFileUpload, onSampleLoad }) => {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [error, setError] = useState<string | null>(null);

  const validateAndUpload = useCallback((file: File) => {
    setError(null);
    if (file.type !== 'text/csv' && !file.name.endsWith('.csv')) {
      setError('Please upload a valid CSV file.');
      return;
    }
    onFileUpload(file);
  }, [onFileUpload]);

  const handleFileInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.[0]) {
      validateAndUpload(e.target.files[0]);
      e.target.value = '';
    }
  }, [validateAndUpload]);

  return (
    <div className="flex-1 flex flex-col items-center justify-center px-4 py-12">
      <div className="max-w-lg text-center space-y-6">
        <h1 className="text-4xl font-extrabold tracking-tight text-slate-900 sm:text-5xl">
          Data cleaning, <br />
          <span className="text-primary-600">reimagined.</span>
        </h1>
        <p className="text-lg text-slate-600">
          Upload your messy CSV files. Our AI agent analyzes, formats, and cleans your data — and you stay in control the whole time.
        </p>

        <div className="flex flex-col sm:flex-row gap-3 justify-center">
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv"
            onChange={handleFileInputChange}
            className="hidden"
          />
          <Button variant="primary" size="lg" onClick={() => fileInputRef.current?.click()}>
            <UploadCloud className="w-5 h-5 mr-2" />
            Upload CSV
          </Button>
          <Button variant="outline" size="lg" onClick={onSampleLoad}>
            <FileSpreadsheet className="w-5 h-5 mr-2" />
            Try Sample Data
          </Button>
        </div>

        {error && (
          <p className="text-sm text-red-600">{error}</p>
        )}

        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 pt-4">
          <div className="p-4 rounded-xl bg-white border border-slate-100 shadow-sm">
            <div className="w-10 h-10 mx-auto bg-blue-50 text-blue-600 rounded-full flex items-center justify-center mb-3">
              <RefreshCw className="w-5 h-5" />
            </div>
            <h3 className="font-semibold text-slate-900">Auto-Format</h3>
            <p className="text-sm text-slate-500 mt-1">Standardizes dates, phones, and currencies.</p>
          </div>
          <div className="p-4 rounded-xl bg-white border border-slate-100 shadow-sm">
            <div className="w-10 h-10 mx-auto bg-green-50 text-green-600 rounded-full flex items-center justify-center mb-3">
              <CheckCircle className="w-5 h-5" />
            </div>
            <h3 className="font-semibold text-slate-900">Validation</h3>
            <p className="text-sm text-slate-500 mt-1">Identifies missing values and outliers.</p>
          </div>
          <div className="p-4 rounded-xl bg-white border border-slate-100 shadow-sm">
            <div className="w-10 h-10 mx-auto bg-purple-50 text-purple-600 rounded-full flex items-center justify-center mb-3">
              <FileSpreadsheet className="w-5 h-5" />
            </div>
            <h3 className="font-semibold text-slate-900">Smart Report</h3>
            <p className="text-sm text-slate-500 mt-1">Generates actionable insights in Markdown.</p>
          </div>
        </div>
      </div>
    </div>
  );
};
