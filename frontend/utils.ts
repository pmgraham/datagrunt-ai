/**
 * Heuristic to distinguish detailed reports from short conversational replies.
 * A message is considered a "report" if it's long or contains markdown section headers.
 */
export function isReportMessage(text: string): boolean {
  if (text.length > 500) return true;
  if (/^#{1,3}\s/m.test(text)) return true;
  return false;
}

/**
 * Removes duplicate report content if the model accidentally output the report twice.
 * Looks for repeated "Executive Summary" headers and truncates at the second occurrence.
 */
export function deduplicateReport(text: string): string {
  // Match "## Executive Summary" or "### Executive Summary" anywhere (not just line start)
  const execSummaryPattern = /#{2,3}\s*Executive Summary/gi;
  const matches = [...text.matchAll(execSummaryPattern)];

  if (matches.length > 1) {
    // Truncate at the start of the second occurrence
    const secondMatchIdx = matches[1].index!;
    return text.substring(0, secondMatchIdx).trim();
  }

  return text;
}
