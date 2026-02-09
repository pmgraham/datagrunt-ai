import React, { useCallback, useState } from 'react';
import { UploadCloud, FileText, X, FileSpreadsheet } from 'lucide-react';
import { clsx } from 'clsx';
import { Button } from './ui/Button';
import { FileData } from '../types';

interface FileUploaderProps {
  onFileProcessed: (fileData: FileData) => void;
}

const SAMPLE_CSV = `ID,Full Name,Email Address,Phone,Join_Date,Department,Salary
1,John Doe,john.doe@example.com,555-123-4567,2023-01-15,Engineering,90000
2,JANE SMITH,jane.smith@test.co,(555) 987-6543,Jan 16 2023,Marketing,
3,Bob Johnson,,555.111.2222,2022/12/01,Sales,65000
4, Alice  Williams ,alice.w@domain.net,555 222 3333,03-15-2023,Engineering,92000
5,,invalid_row,,,
6,Charlie Brown,charlie.brown@email.com,,2023-04-01,Support,45000
7,David Jones,david.jones@example.org,555-444-3333,2023-05-20,Marketing,70000
8,Eve,eve@security.com,555-666-7777,2023-06-10,Security,120000
9,Frank Miller,frank.m@design.io,555-333-2222,2023-02-28,Design,85000
10,Grace Hopper,grace@navy.mil,555-000-1111,1980-12-09,Engineering,150000`;

export const FileUploader: React.FC<FileUploaderProps> = ({ onFileProcessed }) => {
  const [isDragging, setIsDragging] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const processFile = useCallback((file: File) => {
    setError(null);
    if (file.type !== 'text/csv' && !file.name.endsWith('.csv')) {
      setError('Please upload a valid CSV file.');
      return;
    }

    onFileProcessed({
      name: file.name,
      size: file.size,
      file,
    });
  }, [onFileProcessed]);

  const handleSampleLoad = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    setError(null);
    const blob = new Blob([SAMPLE_CSV], { type: 'text/csv' });
    const file = new File([blob], 'sample_employees.csv', { type: 'text/csv' });
    onFileProcessed({
      name: file.name,
      size: file.size,
      file,
    });
  }, [onFileProcessed]);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      processFile(e.dataTransfer.files[0]);
    }
  }, [processFile]);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);

  const handleInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      processFile(e.target.files[0]);
    }
  }, [processFile]);

  return (
    <div
      className={clsx(
        "relative rounded-2xl border-2 border-dashed transition-all duration-200 ease-in-out p-10 flex flex-col items-center justify-center text-center",
        isDragging
          ? "border-primary-500 bg-primary-50/50 scale-[1.01]"
          : "border-slate-300 hover:border-slate-400 bg-slate-50 hover:bg-slate-100/50"
      )}
      onDrop={handleDrop}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
    >
      <div className="w-16 h-16 bg-white rounded-full shadow-sm flex items-center justify-center mb-4 text-primary-600">
        <UploadCloud className="w-8 h-8" />
      </div>

      <h3 className="text-lg font-semibold text-slate-900 mb-1">
        Upload your CSV file
      </h3>
      <p className="text-slate-500 mb-6 max-w-sm">
        Drag and drop your file here, or click to browse. We'll analyze and clean it instantly.
      </p>

      <input
        type="file"
        accept=".csv"
        onChange={handleInputChange}
        className="hidden"
        id="file-upload"
      />
      <label htmlFor="file-upload">
        <Button variant="primary" as="span" className="cursor-pointer">
          Select CSV File
        </Button>
      </label>

      <div className="mt-6 flex items-center w-full max-w-xs">
          <div className="flex-1 h-px bg-slate-200"></div>
          <span className="px-3 text-xs text-slate-400 font-medium">OR</span>
          <div className="flex-1 h-px bg-slate-200"></div>
      </div>

      <button
          onClick={handleSampleLoad}
          className="mt-4 flex items-center text-sm text-primary-600 hover:text-primary-700 font-medium hover:underline focus:outline-none transition-colors"
          type="button"
      >
          <FileSpreadsheet className="w-4 h-4 mr-2" />
          Load messy sample data
      </button>

      {error && (
        <div className="mt-6 p-3 bg-red-50 text-red-600 rounded-lg text-sm flex items-center animate-pulse">
          <X className="w-4 h-4 mr-2" />
          {error}
        </div>
      )}
    </div>
  );
};
