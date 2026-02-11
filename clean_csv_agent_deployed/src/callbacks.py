# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Callbacks for the clean-csv-agent system."""

import tempfile
from typing import Optional

from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types


# MIME types that indicate a CSV or spreadsheet file upload
_CSV_MIME_TYPES = (
    "text/csv",
    "text/plain",
    "application/csv",
    "application/vnd.ms-excel",
    "application/octet-stream",
)


def intercept_file_upload(
    callback_context: CallbackContext, llm_request: LlmRequest
) -> Optional[LlmResponse]:
    """Before-model callback that intercepts CSV file uploads.

    When a user uploads a file via the UI (paperclip icon), the raw bytes
    arrive as inline_data Parts in the model request. Without interception,
    the entire CSV text is sent through the LLM context window — wasting
    tokens and confusing the agent.

    This callback:
    1. Detects inline_data Parts with CSV-like MIME types
    2. Saves the file bytes to a temp file on disk (first upload only)
    3. Stores the path in session state so load_csv can find it
    4. Replaces the bulky inline_data with a short text instruction

    This runs on every model call to ensure file bytes never reach the LLM,
    even when conversation history is replayed.
    """
    file_saved = callback_context.state.get("_file_upload_processed", False)

    new_contents = []
    for content in llm_request.contents:
        new_parts = []
        modified = False

        for part in content.parts:
            # Check for inline file data (from paperclip upload)
            if part.inline_data and part.inline_data.data:
                mime = (part.inline_data.mime_type or "").lower()
                if any(csv_mime in mime for csv_mime in _CSV_MIME_TYPES):
                    modified = True

                    # Save bytes to disk on first encounter only
                    if not file_saved:
                        tmp = tempfile.NamedTemporaryFile(
                            mode="wb", suffix=".csv", delete=False
                        )
                        tmp.write(part.inline_data.data)
                        tmp.close()
                        callback_context.state["csv_path"] = tmp.name
                        callback_context.state["_file_upload_processed"] = True
                        file_saved = True

                    # Replace bulky file data with lightweight text
                    new_parts.append(types.Part.from_text(
                        text=(
                            "[User uploaded a CSV file. It has been saved "
                            "and is ready to load. Call load_csv with no "
                            "file_path argument to begin analysis.]"
                        )
                    ))
                    continue

            new_parts.append(part)

        if modified:
            new_contents.append(
                types.Content(role=content.role, parts=new_parts)
            )
        else:
            new_contents.append(content)

    llm_request.contents = new_contents
    return None  # Continue to model
