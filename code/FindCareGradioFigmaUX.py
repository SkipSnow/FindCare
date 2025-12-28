# =============================================================================
# File: FindCareGradioFigmaUX.py
# Author: Skip Snow
# Co-Author: GPT-5
# Copyright (c) 2025 Skip Snow. All rights reserved.
# =============================================================================

from __future__ import annotations

import html
import logging
import os
import re
import threading
import time
import uuid
import webbrowser
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

# Optional imports (best-effort)
try:
    from PIL import Image  # type: ignore
except Exception:
    Image = None  # type: ignore

try:
    import pytesseract  # type: ignore
except Exception:
    pytesseract = None  # type: ignore

try:
    import PyPDF2  # type: ignore
except Exception:
    PyPDF2 = None  # type: ignore

try:
    import gradio as gr  # type: ignore
except Exception:
    gr = None  # type: ignore


logger = logging.getLogger("findcare")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


class FindCareGradioFigmaUX:
    """
    Single-file, class-based wrapper for the FindCare FastAPI + Gradio UI.

    Constructor responsibilities (per spec):
      - Accept argIP (string) and argPort (string)
      - Initialize FastAPI routes + Gradio UI sufficiently so the first page can be served
      - Start both servers (Uvicorn hosting the combined FastAPI+Gradio app)
      - Open the home page that is first rendered today (root "/" which redirects to the Gradio UI path)
    """

    # UI is mounted under this path; "/" redirects here (no splash page).
    UI_PATH: str = "/gradio"

    # Allowed origins for local frontends (kept from notebook).
    ALLOWED_ORIGINS: List[str] = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]

    DEFAULT_SUMMARY_INTERVAL_SEC: int = int(os.getenv("FINDCARE_SUMMARY_INTERVAL_SEC", "60"))

    # Minimal mock provider corpus (kept from the notebook; extend/replace with real data sources later)
    MOCK_PROVIDERS: List[Dict[str, Any]] = [
        {
            "id": "prov-0001",
            "name": "Ava Chen, MD",
            "specialty": "Family Medicine",
            "city": "Los Angeles",
            "state": "CA",
            "rating": 4.7,
            "accepts_insurance": ["Aetna", "Blue Shield", "United"],
            "notes": "",
        },
        {
            "id": "prov-0002",
            "name": "Noah Patel, DO",
            "specialty": "Dermatology",
            "city": "Santa Monica",
            "state": "CA",
            "rating": 4.6,
            "accepts_insurance": ["Cigna", "Blue Shield"],
            "notes": "",
        },
        {
            "id": "prov-0003",
            "name": "Mia Johnson, MD",
            "specialty": "Cardiology",
            "city": "Pasadena",
            "state": "CA",
            "rating": 4.8,
            "accepts_insurance": ["Aetna", "United"],
            "notes": "",
        },
        {
            "id": "prov-0004",
            "name": "Ethan Park, MD",
            "specialty": "Orthopedic Surgery",
            "city": "Burbank",
            "state": "CA",
            "rating": 4.5,
            "accepts_insurance": ["Blue Shield", "United"],
            "notes": "",
        },
    ]

    US_STATES: List[Tuple[str, str]] = [
        ("AL", "Alabama"), ("AK", "Alaska"), ("AZ", "Arizona"), ("AR", "Arkansas"), ("CA", "California"),
        ("CO", "Colorado"), ("CT", "Connecticut"), ("DE", "Delaware"), ("FL", "Florida"), ("GA", "Georgia"),
        ("HI", "Hawaii"), ("ID", "Idaho"), ("IL", "Illinois"), ("IN", "Indiana"), ("IA", "Iowa"),
        ("KS", "Kansas"), ("KY", "Kentucky"), ("LA", "Louisiana"), ("ME", "Maine"), ("MD", "Maryland"),
        ("MA", "Massachusetts"), ("MI", "Michigan"), ("MN", "Minnesota"), ("MS", "Mississippi"), ("MO", "Missouri"),
        ("MT", "Montana"), ("NE", "Nebraska"), ("NV", "Nevada"), ("NH", "New Hampshire"), ("NJ", "New Jersey"),
        ("NM", "New Mexico"), ("NY", "New York"), ("NC", "North Carolina"), ("ND", "North Dakota"), ("OH", "Ohio"),
        ("OK", "Oklahoma"), ("OR", "Oregon"), ("PA", "Pennsylvania"), ("RI", "Rhode Island"), ("SC", "South Carolina"),
        ("SD", "South Dakota"), ("TN", "Tennessee"), ("TX", "Texas"), ("UT", "Utah"), ("VT", "Vermont"),
        ("VA", "Virginia"), ("WA", "Washington"), ("WV", "West Virginia"), ("WI", "Wisconsin"), ("WY", "Wyoming"),
    ]

    CSS: str = """
    :root { --fc-border:#d0d7de; --fc-bg:#ffffff; --fc-header:#4682b4; --fc-text:#111; }
    .fc-frame { border:1px solid var(--fc-border); border-radius:14px; padding:12px; background:var(--fc-bg); }
    .fc-header { border:1px solid var(--fc-border); border-radius:14px; padding:10px 12px; background:var(--fc-header); color:white; }
    .fc-header a { color:white; text-decoration:none; font-weight:600; font-size:13px; }
    .fc-header a:hover { text-decoration:underline; }
    .fc-logo { font-weight:700; font-size:14px; letter-spacing:0.3px; }
    .fc-subtle { font-size:12px; opacity:0.92; }
    """

    def __init__(self, argIP: str, argPort: str):
        if not isinstance(argIP, str) or not argIP.strip():
            raise ValueError("argIP must be a non-empty string")
        if not isinstance(argPort, str) or not argPort.strip():
            raise ValueError("argPort must be a non-empty string")

        self.host: str = argIP.strip()
        self.port_str: str = argPort.strip()
        try:
            self.port: int = int(self.port_str)
        except Exception as e:
            raise ValueError(f"argPort must parse to an int; got '{argPort}'") from e

        if gr is None:
            raise RuntimeError("gradio is not available. Install with: pip install gradio")

        self._server_thread: Optional[threading.Thread] = None

        # Build the FastAPI app and register routes.
        self.app: FastAPI = FastAPI(title="FindCare Gradio Backend (FastAPI API Layer)", version="1.0")
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=self.ALLOWED_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self._register_routes()

        # Build Gradio UI and mount it.
        self.demo = self._build_gradio_ui()
        self.app = gr.mount_gradio_app(self.app, self.demo, path=self.UI_PATH)

        # Start Uvicorn (hosting combined FastAPI+Gradio app) and open browser.
        self._start_server_and_open_browser(open_path="/")

        print(f"FindCare started. Home: http://{self.host}:{self.port}/  (Gradio UI at {self.UI_PATH})")

    # ----------------------------
    # Helpers
    # ----------------------------
    @staticmethod
    def utc_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def new_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"

    @staticmethod
    def sanitize_html_allow_basic(raw: str) -> str:
        """
        Very small HTML sanitizer that allows basic tags and strips scripts/events.
        This is intentionally conservative for an MVP.
        """
        if not raw:
            return ""
        raw = re.sub(r"(?is)<script.*?>.*?</script>", "", raw)
        raw = re.sub(r"on\w+\s*=\s*(['\"]).*?\1", "", raw, flags=re.I)
        return raw

    @staticmethod
    def _build_header_html() -> str:
        # Links are server-resolved pages (served by FastAPI routes below)
        return """
        <div class="fc-header" style="display:flex;align-items:center;gap:18px;justify-content:space-between;">
          <div class="fc-logo">FindCare</div>
          <div style="display:flex;gap:18px;align-items:center;">
            <a href="/secret-sause" target="_blank" title="Tools and...to fuel this AI application. (full disclosure)">Secret Sause</a>
            <a href="/about" target="_blank" title="about 'go to the about Find Care page.'">About</a>
            <a href="mailto:skip.snow@gmail.com?subject=FindCare%20I... Skip Snow (From variable) from Find Care">Contact Find Care</a>
            <a href="/privacy" target="_blank" title="Get Find Care's Privacy Policy">Privacy policy</a>
          </div>
        </div>
        """

    @classmethod
    def _filter_providers(
        cls,
        state: Optional[str] = None,
        city: Optional[str] = None,
        specialty: Optional[str] = None,
        insurance: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        results = list(cls.MOCK_PROVIDERS)
        if state:
            results = [p for p in results if (p.get("state") or "").upper() == state.strip().upper()]
        if city:
            c = city.strip().lower()
            results = [p for p in results if c in (p.get("city") or "").lower()]
        if specialty:
            s = specialty.strip().lower()
            results = [p for p in results if s in (p.get("specialty") or "").lower()]
        if insurance:
            ins = insurance.strip()
            results = [p for p in results if ins in (p.get("accepts_insurance") or [])]
        return results[: max(1, int(limit))]

    @staticmethod
    def build_provider_table(providers: List[Dict[str, Any]], page: int = 1, page_size: int = 25) -> Dict[str, Any]:
        page = max(1, int(page))
        page_size = max(1, int(page_size))
        start = (page - 1) * page_size
        subset = providers[start : start + page_size]

        headers = [
            {"key": "name", "label": "Provider Name", "sortable": True, "editable": False},
            {"key": "specialty", "label": "Specialty", "sortable": True, "editable": False},
            {"key": "city", "label": "City", "sortable": True, "editable": False},
            {"key": "state", "label": "State", "sortable": True, "editable": False},
            {"key": "rating", "label": "Rating", "sortable": True, "editable": False},
            {"key": "notes", "label": "Notes", "sortable": False, "editable": True},
        ]

        rows: List[Dict[str, Any]] = []
        for p in subset:
            rows.append(
                {
                    "id": p.get("id"),
                    "name": p.get("name"),
                    "specialty": p.get("specialty"),
                    "city": p.get("city"),
                    "state": p.get("state"),
                    "rating": p.get("rating"),
                    "notes": p.get("notes", ""),
                }
            )

        return {
            "type": "provider-table",
            "page": page,
            "page_size": page_size,
            "total": len(providers),
            "headers": headers,
            "rows": rows,
        }

    # ----------------------------
    # FastAPI routes
    # ----------------------------
    def _register_routes(self) -> None:
        @self.app.post("/api/header")
        async def header_api(payload: Dict[str, Any]) -> JSONResponse:
            link = (payload or {}).get("link")
            if link not in {"secret-sause", "about", "contact", "privacy-policy"}:
                return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid link"})

            if link == "contact":
                return JSONResponse(
                    content={
                        "type": "contact-info",
                        "contactName": "Skip Snow",
                        "email": "skip.snow@gmail.com",
                        "message": "Use the mailto link in the header to contact FindCare.",
                    }
                )

            # Map to page endpoints for browser
            target = {
                "secret-sause": "/secret-sause",
                "about": "/about",
                "privacy-policy": "/privacy",
            }.get(link, "/")
            return JSONResponse(content={"type": "nav", "href": target})

        @self.app.post("/api/session-summary")
        async def session_summary_api(payload: Dict[str, Any]) -> JSONResponse:
            print(" session_summary_api called")
            # Placeholder: client can call on timer
            return JSONResponse(
                content={
                    "type": "session-summary",
                    "ts": self.utc_iso(),
                    "summary": "MVP: session summary not yet implemented (wireframe placeholder).",
                    "interval_seconds": self.DEFAULT_SUMMARY_INTERVAL_SEC,
                }
            )

        @self.app.post("/api/button-manager")
        async def button_manager_api(payload: Dict[str, Any]) -> JSONResponse:
            # Placeholder for button manager semantics.
            return JSONResponse(
                content={
                    "type": "button-manager",
                    "ts": self.utc_iso(),
                    "echo": payload or {},
                    "status": "ok",
                }
            )

        @self.app.post("/api/scrollable-output")
        async def scrollable_output_api(payload: Dict[str, Any]) -> JSONResponse:
            # Placeholder: return a chat-like entry.
            message = (payload or {}).get("message", "")
            return JSONResponse(
                content={
                    "type": "scrollable-output",
                    "ts": self.utc_iso(),
                    "items": [
                        {"role": "assistant", "content": f"(MVP) Received: {message[:4000]}"},
                    ],
                }
            )

        @self.app.post("/api/graphic-content")
        async def graphic_content_api(payload: Dict[str, Any]) -> JSONResponse:
            # Placeholder: return a small card payload.
            return JSONResponse(
                content={
                    "type": "graphic-content",
                    "ts": self.utc_iso(),
                    "content": {
                        "title": "Graphic Content (placeholder)",
                        "body": "Wireframe slot for clickable image regions / data-driven graphics.",
                    },
                }
            )

        @self.app.post("/api/prompt")
        async def prompt_api(payload: Dict[str, Any]) -> JSONResponse:
            """
            Primary prompt endpoint. For MVP we demo provider filtering + return a provider table.
            """
            criteria = payload or {}
            state = criteria.get("state")
            city = criteria.get("city")
            specialty = criteria.get("specialty")
            insurance = criteria.get("insurance")
            limit = int(criteria.get("limit", 50))

            providers = self._filter_providers(state=state, city=city, specialty=specialty, insurance=insurance, limit=limit)
            table = self.build_provider_table(providers, page=1, page_size=min(25, max(1, limit)))

            return JSONResponse(
                content={
                    "type": "prompt-result",
                    "ts": self.utc_iso(),
                    "providers": table,
                    "notes": "MVP response from mock data. Replace _filter_providers with DB-backed logic.",
                }
            )

        @self.app.get("/health")
        async def health() -> JSONResponse:
            return JSONResponse(content={"status": "ok", "ts": self.utc_iso(), "ui_path": self.UI_PATH})

        @self.app.get("/")
        async def root() -> RedirectResponse:
            # No splash page: go straight to the first UI screen.
            return RedirectResponse(url=self.UI_PATH)

        @self.app.get("/about")
        async def about_page() -> HTMLResponse:
            page = """
            <html><head><title>About FindCare</title></head>
            <body style="font-family:system-ui,Segoe UI,Arial;max-width:900px;margin:40px auto;line-height:1.5;">
              <h2>About FindCare</h2>
              <p>FindCare is a healthcare AI assistant prototype focused on guiding users to ask good questions
              within the provider & specialty domain, and returning results using available datasets.</p>
              <p><a href="/">Back to FindCare</a></p>
            </body></html>
            """
            return HTMLResponse(content=page)

        @self.app.get("/secret-sause")
        async def secret_sause_page() -> HTMLResponse:
            page = """
            <html><head><title>Secret Sause</title></head>
            <body style="font-family:system-ui,Segoe UI,Arial;max-width:900px;margin:40px auto;line-height:1.5;">
              <h2>Secret Sause</h2>
              <p>This page lists the tools and techniques used to fuel this AI application (full disclosure).</p>
              <ul>
                <li>FastAPI for the API layer</li>
                <li>Gradio for the UX harness</li>
                <li>Provider & specialty datasets supplied by the project</li>
              </ul>
              <p><a href="/">Back to FindCare</a></p>
            </body></html>
            """
            return HTMLResponse(content=page)

        @self.app.get("/privacy")
        async def privacy_page() -> HTMLResponse:
            content = self.sanitize_html_allow_basic(
                "<p><strong>Find Care Privacy Policy (MVP)</strong></p>"
                "<p>FindCare does not store passwords. Identified PHI may be accessed only with explicit user action "
                "and is not retained by default.</p>"
            )
            page = f"""
            <html><head><title>Privacy Policy</title></head>
            <body style="font-family:system-ui,Segoe UI,Arial;max-width:900px;margin:40px auto;line-height:1.5;">
              {content}
              <p><a href="/">Back to FindCare</a></p>
            </body></html>
            """
            return HTMLResponse(content=page)

    # ----------------------------
    # Gradio UI
    # ----------------------------
    def _build_gradio_ui(self):
        # Build a UI that matches the "frames" layout from the notebook.
        with gr.Blocks(css=self.CSS, title="FindCare") as demo:
            gr.HTML(self._build_header_html())

            # Frame 1: Provider Table section
            with gr.Group(elem_classes=["fc-frame"]):
                gr.Markdown("### Providers Table (wireframe)")
                table_json = gr.JSON(
                    label="(Server returns table JSON to be rendered by a browser client later)",
                    value=self.build_provider_table(self.MOCK_PROVIDERS, page=1, page_size=25),
                )

            # Frame 2: Button Manager
            with gr.Row():
                with gr.Column(scale=1):
                    with gr.Group(elem_classes=["fc-frame"]):
                        gr.Markdown("### Button Manager")
                        btn_search = gr.Button("Search (mock)")
                        btn_clear = gr.Button("Clear")
                        btn_status = gr.Textbox(label="", interactive=False, value="(button events routed client-side later)")

            # Frame 3: Server Output (non-scroll)
            with gr.Row():
                with gr.Column(scale=1):
                    with gr.Group(elem_classes=["fc-frame"]):
                        gr.Markdown("### Server Output")
                        server_output = gr.Textbox(
                            label="",
                            value="(Server will return color-attributed results array for the browser client.)",
                            lines=8,
                            interactive=False,
                            show_copy_button=True,
                        )

            # Frame 4: Scrollable Output (Chat)
            with gr.Row():
                with gr.Column(scale=1):
                    with gr.Group(elem_classes=["fc-frame"]):
                        gr.Markdown("### Scrollable Output")
                        chat = gr.Chatbot(height=320, label="", show_copy_button=True)

            # Frame 5: Graphic Content
            with gr.Row():
                with gr.Column(scale=1):
                    with gr.Group(elem_classes=["fc-frame"]):
                        gr.Markdown("### Graphic Content")
                        graphic = gr.Markdown(
                            "(wireframe) Slot for data-driven graphic / clickable image regions.",
                        )

            # Frame 6: Prompt
            with gr.Row():
                with gr.Column(scale=1):
                    with gr.Group(elem_classes=["fc-frame"]):
                        gr.Markdown("### Prompt")
                        prompt = gr.Textbox(
                            label="",
                            placeholder="Ask about healthcare providers… (sent only on Send / Enter)",
                            lines=4,
                        )
                        files = gr.File(label="Upload files (images/PDFs)", file_count="multiple")
                        send = gr.Button("Send")
                        prompt_status = gr.Textbox(label="", interactive=False, value="")

            # ---------- Local harness wiring ----------
            def ui_clear():
                return self.build_provider_table(self.MOCK_PROVIDERS, page=1, page_size=25), "", [], "(cleared)"

            def ui_search():
                # Demonstrate a round-trip to the filtering logic (still local harness).
                providers = self._filter_providers(state="CA", limit=50)
                return self.build_provider_table(providers, page=1, page_size=25), "(mock search ran)"

            def ui_send_message(p: str, history: List[Tuple[str, str]]):
                p = (p or "").strip()
                if not p:
                    return history, "", "Please enter a prompt."
                history = history or []
                history.append((p, "Thanks — backend received your prompt (local harness response)."))
                return history, "", ""

            btn_clear.click(ui_clear, inputs=None, outputs=[table_json, prompt, chat, btn_status])
            btn_search.click(ui_search, inputs=None, outputs=[table_json, btn_status])

            send.click(ui_send_message, inputs=[prompt, chat], outputs=[chat, prompt, prompt_status])
            prompt.submit(ui_send_message, inputs=[prompt, chat], outputs=[chat, prompt, prompt_status])

        return demo

    # ----------------------------
    # Uvicorn startup
    # ----------------------------
    def _run_server(self) -> None:
        import uvicorn  # local import so the module can be imported without uvicorn installed in some contexts

        # Print registered routes for quick debugging
        try:
            routes = []
            for r in self.app.routes:
                if hasattr(r, "path") and hasattr(r, "methods"):
                    routes.append((getattr(r, "path", ""), sorted(list(getattr(r, "methods", []) or []))))
            logger.info("Registered routes: %s", routes)
        except Exception:
            pass

        uvicorn.run(self.app, host=self.host, port=self.port, log_level="info")

    def _start_server_and_open_browser(self, open_path: str = "/") -> None:
        if self._server_thread and self._server_thread.is_alive():
            url = f"http://{self.host}:{self.port}{open_path}"
            print(f"Server already running on {url}")
            return

        self._server_thread = threading.Thread(target=self._run_server, daemon=True)
        self._server_thread.start()

        # Give Uvicorn a moment to bind before opening the browser.
        time.sleep(1.0)
        url = f"http://{self.host}:{self.port}{open_path}"
        print(f"Opening: {url}")
        try:
            webbrowser.open(url, new=1)
        except Exception as e:
            print(f"Could not open browser automatically: {e}\nOpen manually: {url}")


if __name__ == "__main__":
    # Convenience entrypoint:
    #   python FindCareGradioFigmaUX.py
    # Uses env vars if present, else defaults.
    ip = os.getenv("FINDCARE_HOST", "127.0.0.1")
    port = os.getenv("FINDCARE_PORT", "7860")
    FindCareGradioFigmaUX(ip, port)
