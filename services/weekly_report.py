"""Service layer for weekly report generation and retrieval."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from langchain_core.messages import HumanMessage
from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer

from core.graph import get_langgraph_app

logger = logging.getLogger("uvicorn.error")

REPORTS_DIR = Path(".reports")
METADATA_FILENAME = "metadata.json"
LOG_DIRNAME = "log"

LOG_METADATA_ENV = "WEEKLY_REPORT_LOG_METADATA"


def _normalize_language(language: str) -> str:
	if not language:
		return "fr"
	return "en" if language.strip().lower().startswith("en") else "fr"


def _normalize_report_type(report_type: str) -> str:
	value = (report_type or "").strip().lower().replace("-", "_")
	if value in {"weak_signal", "weak_signals", "weaksignal"}:
		return "weak_signal"
	return "hotspot"


def _split_report_and_recommendations_text(content: str, language: str) -> tuple[str, str]:
	marker = "Recommandations :" if language == "fr" else "Recommendations:"
	if not isinstance(content, str):
		return "", ""
	idx = content.find(marker)
	if idx < 0:
		return content.strip(), ""
	return content[:idx].strip(), content[idx + len(marker) :].strip()


def _extract_json_payload(raw_content: str) -> Dict[str, str]:
	if not isinstance(raw_content, str):
		return {}
	content = raw_content.strip()
	if not content:
		return {}
	try:
		payload = json.loads(content)
		if isinstance(payload, dict):
			return payload
	except Exception:
		pass

	start = content.find("{")
	end = content.rfind("}")
	if start >= 0 and end > start:
		try:
			payload = json.loads(content[start : end + 1])
			if isinstance(payload, dict):
				return payload
		except Exception:
			return {}
	return {}


def _fallback_recommendations(language: str, report_type: str) -> str:
	fallbacks = {
		"fr": {
			"hotspot": "- Prioriser des interventions ciblees (signalisation, marquage) sur les zones touchees.\n- Renforcer la prevention aux periodes critiques observees.",
			"weak_signal": "- Surveiller de pres les zones en hausse pour confirmer la tendance.\n- Deployer des actions preventives legeres des la semaine suivante.",
		},
		"en": {
			"hotspot": "- Prioritize targeted interventions (signage, markings) in the most affected areas.\n- Increase prevention during critical time windows.",
			"weak_signal": "- Closely monitor rising zones to confirm or reject the trend.\n- Deploy light preventive actions in the following week.",
		},
	}
	return fallbacks.get(language, fallbacks["fr"]).get(report_type, fallbacks["fr"]["hotspot"])


@dataclass
class ReportContent:
	report: str
	recommendations: str


class IReportGenerator(ABC):
	@abstractmethod
	def generate(self, report_type: str, language: str) -> ReportContent:
		raise NotImplementedError


class IPDFBuilder(ABC):
	@abstractmethod
	def build_unified_pdf(
		self,
		language: str,
		hotspot: ReportContent,
		weak_signal: ReportContent,
		output_path: Path,
		generated_at: str,
	) -> Path:
		raise NotImplementedError


class IReportStorage(ABC):
	@abstractmethod
	def get_output_path(self, language: str, date_tag: str) -> Path:
		raise NotImplementedError

	@abstractmethod
	def update_metadata(self, language: str, pdf_path: Path, generated_at: str) -> None:
		raise NotImplementedError

	@abstractmethod
	def get_latest(self, language: str) -> Optional[Dict[str, str]]:
		raise NotImplementedError


class LangGraphReportGenerator(IReportGenerator):
	def __init__(self):
		self._app = get_langgraph_app()

	def generate(self, report_type: str, language: str) -> ReportContent:
		report_type = _normalize_report_type(report_type)
		language = _normalize_language(language)
		today = time.strftime("%Y-%m-%d")

		no_data_messages = {
			"hotspot": {
				"fr": "Aucun incident ou requete majeure n'a ete enregistre dans nos bases de donnees pour cette semaine.",
				"en": "No major incidents or requests were recorded in our databases this week.",
			},
			"weak_signal": {
				"fr": "Aucun signal faible detecte cette semaine.",
				"en": "No weak signals detected this week.",
			},
		}

		no_data_message = no_data_messages[report_type][language]
		if report_type == "hotspot":
			prompt = self._build_hotspot_prompt(today, language, no_data_message)
		else:
			prompt = self._build_weak_signal_prompt(today, language, no_data_message)

		initial_state = {
			"messages": [HumanMessage(content=prompt)],
			"audience": "grand_public",
			"is_ambiguous": False,
		}

		result = self._app.invoke(initial_state)
		raw_content = result.get("analytical_response", "")

		payload = _extract_json_payload(raw_content)
		report = payload.get("report") or raw_content
		recommendations = payload.get("recommendations", "")

		if not isinstance(report, str) or not report.strip():
			report = "Generation error."

		if not isinstance(recommendations, str):
			recommendations = ""

		if report.strip() != no_data_message and not recommendations.strip():
			_, recommendations = _split_report_and_recommendations_text(report, language)
			if not recommendations.strip():
				recommendations = _fallback_recommendations(language, report_type)

		report, extracted_reco = _split_report_and_recommendations_text(report, language)
		if extracted_reco and not recommendations.strip():
			recommendations = extracted_reco

		return ReportContent(report=report.strip(), recommendations=recommendations.strip())

	def _build_hotspot_prompt(self, today: str, language: str, no_data_message: str) -> str:
		language_label = "French" if language == "fr" else "English"
		return f"""
You are the Lead Data Analyst for Montreal Mobility.
Generate the WEEKLY MOBILITY HOTSPOTS BRIEFING based on the last 7 days. Today is {today}.

TASK:
1. Query the database to find data for the LAST 7 DAYS.
2. Identify the TOP 5 absolute worst hotspots based on the highest concentration of collisions or 311 requests.

GEOGRAPHY & SQL RULES (CRITICAL):
- NEVER group by or display raw X/Y coordinates, Lat/Long, or points.
- For collisions, group by 'ACCDN_PRES_DE' or 'RUE_ACCDN'.
- For 311 requests, DO NOT just group by Borough. You MUST group by Borough AND the specific issue topic.
- SQL TIP FOR 311: `GROUP BY ARRONDISSEMENT, NATURE` (or whichever column contains the specific issue).

CRITICAL ANTI-HALLUCINATION RULES:
- You MUST rely EXCLUSIVELY on the data retrieved by your tools.
- DO NOT invent weather, times, or durations.
- If there is no data, `report` must be exactly: "{no_data_message}"

STRICT FORMATTING RULES (Only for REAL data):
- Use this format:
  "Hotspot #[Rank] : [Intersection/Zone name] - [Total Number] [SPECIFIC Type of Issue] ([Any real details from DB]), [Weather/Time IF retrieved]."
- ABSOLUTELY NO GPS COORDINATES IN THE TEXT.

RECOMMENDATIONS SECTION:
- Provide 2 to 4 short, actionable, and preventive recommendations tied to the hotspots.

LANGUAGE:
- Write the report and recommendations in {language_label} only.

OUTPUT FORMAT (CRITICAL):
You MUST output ONLY a valid JSON object. No markdown formatting:
{{
	"report": "...",
	"recommendations": "..."
}}
"""

	def _build_weak_signal_prompt(self, today: str, language: str, no_data_message: str) -> str:
		language_label = "French" if language == "fr" else "English"
		return f"""
You are the Lead Data Analyst for Montreal Mobility.
Generate the WEEKLY MOBILITY WEAK SIGNALS BRIEFING for the last 7 days. Today is {today}.

BUSINESS RULES:
- A 'weak signal' is a small early change that may indicate a future problem.
- Query the database for the last 7 days using SQLite syntax: `WHERE date_column >= date('{today}', '-7 days')`.
- Compare this to historical averages if necessary.
- If there are no weak signals, `report` must be exactly: "{no_data_message}"
- DO NOT INVENT SIGNALS.

RECOMMENDATIONS SECTION:
- Provide 2 to 4 short, preventive recommendations based on the weak signals identified.

LANGUAGE:
- Write the report and recommendations in {language_label} only.

OUTPUT FORMAT (CRITICAL):
You MUST output ONLY a valid JSON object. No markdown formatting:
{{
	"report": "...",
	"recommendations": "..."
}}
"""


class ReportLabPDFBuilder(IPDFBuilder):
	def build_unified_pdf(
		self,
		language: str,
		hotspot: ReportContent,
		weak_signal: ReportContent,
		output_path: Path,
		generated_at: str,
	) -> Path:
		language = _normalize_language(language)
		styles = getSampleStyleSheet()

		title_style = ParagraphStyle(
			"TitleStyle",
			parent=styles["Title"],
			textColor=colors.HexColor("#0F172A"),
			spaceAfter=12,
		)
		subtitle_style = ParagraphStyle(
			"SubtitleStyle",
			parent=styles["Normal"],
			textColor=colors.HexColor("#334155"),
			fontSize=11,
			spaceAfter=18,
		)
		section_style = ParagraphStyle(
			"SectionStyle",
			parent=styles["Heading2"],
			textColor=colors.HexColor("#0B4F6C"),
			spaceBefore=12,
			spaceAfter=8,
		)
		body_style = ParagraphStyle(
			"BodyStyle",
			parent=styles["BodyText"],
			leading=14,
		)
		reco_style = ParagraphStyle(
			"RecoStyle",
			parent=styles["BodyText"],
			textColor=colors.HexColor("#0F172A"),
			leading=14,
		)

		title = "Rapport Hebdomadaire - Mobilite Montreal" if language == "fr" else "Weekly Report - Montreal Mobility"
		subtitle = f"{generated_at}"

		section_hotspot = "Points chauds" if language == "fr" else "Hotspots"
		section_weak_signal = "Signaux faibles" if language == "fr" else "Weak signals"
		recommendations_label = "Recommandations" if language == "fr" else "Recommendations"

		def format_paragraph(text: str) -> str:
			return (text or "").replace("\n", "<br/>")

		story = [
			Paragraph(title, title_style),
			Paragraph(subtitle, subtitle_style),
			Spacer(1, 0.2 * inch),
			Paragraph(section_hotspot, section_style),
			Paragraph(format_paragraph(hotspot.report), body_style),
			Spacer(1, 0.15 * inch),
			Paragraph(recommendations_label, section_style),
			Paragraph(format_paragraph(hotspot.recommendations), reco_style),
			PageBreak(),
			Paragraph(section_weak_signal, section_style),
			Paragraph(format_paragraph(weak_signal.report), body_style),
			Spacer(1, 0.15 * inch),
			Paragraph(recommendations_label, section_style),
			Paragraph(format_paragraph(weak_signal.recommendations), reco_style),
		]

		def add_page_number(canvas, doc):
			canvas.saveState()
			canvas.setFont("Helvetica", 9)
			canvas.setFillColor(colors.HexColor("#64748B"))
			canvas.drawRightString(LETTER[0] - 0.6 * inch, 0.5 * inch, f"Page {doc.page}")
			canvas.restoreState()

		output_path.parent.mkdir(parents=True, exist_ok=True)
		doc = SimpleDocTemplate(
			str(output_path),
			pagesize=LETTER,
			leftMargin=0.75 * inch,
			rightMargin=0.75 * inch,
			topMargin=0.75 * inch,
			bottomMargin=0.75 * inch,
		)
		doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
		return output_path


class FileSystemReportStorage(IReportStorage):
	def __init__(self, base_dir: Path = REPORTS_DIR, log_metadata: bool = False) -> None:
		self.base_dir = base_dir
		self.log_metadata = log_metadata
		self.log_dir = self.base_dir / LOG_DIRNAME
		self.lock = threading.RLock()

	def get_output_path(self, language: str, date_tag: str) -> Path:
		language = _normalize_language(language)
		filename = f"{date_tag}_weekly_report_{language}.pdf"
		with self.lock:
			self.base_dir.mkdir(parents=True, exist_ok=True)
			self._cleanup_old_reports(language)
			return self.base_dir / filename

	def update_metadata(self, language: str, pdf_path: Path, generated_at: str) -> None:
		if not self.log_metadata:
			return
		language = _normalize_language(language)
		with self.lock:
			self.base_dir.mkdir(parents=True, exist_ok=True)
			self.log_dir.mkdir(parents=True, exist_ok=True)
			self._archive_existing_metadata()
			payload = self._load_metadata()
			payload["updated_at"] = generated_at
			latest = payload.setdefault("latest", {})
			latest[language] = {
				"path": str(pdf_path),
				"generated_at": generated_at,
				"sections": ["hotspot", "weak_signal"],
			}
			self._write_metadata(payload)

	def get_latest(self, language: str) -> Optional[Dict[str, str]]:
		language = _normalize_language(language)
		metadata_path = self.base_dir / METADATA_FILENAME
		if metadata_path.exists():
			payload = self._load_metadata()
			latest = payload.get("latest", {})
			entry = latest.get(language)
			if isinstance(entry, dict) and entry.get("path"):
				return entry

		pattern = f"*_weekly_report_{language}.pdf"
		candidates = list(self.base_dir.glob(pattern))
		if not candidates:
			return None
		latest_file = max(candidates, key=lambda path: path.stat().st_mtime)
		generated_at = datetime.fromtimestamp(latest_file.stat().st_mtime).strftime("%Y-%m-%dT%H:%M:%S")
		return {
			"path": str(latest_file),
			"generated_at": generated_at,
			"sections": ["hotspot", "weak_signal"],
		}

	def _cleanup_old_reports(self, language: str) -> None:
		pattern = f"*_weekly_report_{language}.pdf"
		for path in self.base_dir.glob(pattern):
			try:
				path.unlink()
			except OSError:
				logger.exception("Failed to remove old report: %s", path)

	def _archive_existing_metadata(self) -> None:
		metadata_path = self.base_dir / METADATA_FILENAME
		if not metadata_path.exists():
			return
		timestamp = time.strftime("%Y%m%d_%H%M%S")
		archived_name = f"metadata_{timestamp}.json"
		archived_path = self.log_dir / archived_name
		metadata_path.replace(archived_path)

	def _load_metadata(self) -> Dict[str, Dict[str, Dict[str, str]]]:
		metadata_path = self.base_dir / METADATA_FILENAME
		if not metadata_path.exists():
			return {"updated_at": "", "latest": {}}
		try:
			with metadata_path.open("r", encoding="utf-8") as file:
				payload = json.load(file)
			if isinstance(payload, dict):
				return payload
		except Exception:
			logger.exception("Failed to read metadata")
		return {"updated_at": "", "latest": {}}

	def _write_metadata(self, payload: Dict[str, Dict[str, Dict[str, str]]]) -> None:
		metadata_path = self.base_dir / METADATA_FILENAME
		with metadata_path.open("w", encoding="utf-8") as file:
			json.dump(payload, file, ensure_ascii=False, indent=2)


class WeeklyReportService:
	def __init__(
		self,
		generator: IReportGenerator,
		pdf_builder: IPDFBuilder,
		storage: IReportStorage,
	) -> None:
		self.generator = generator
		self.pdf_builder = pdf_builder
		self.storage = storage

	def generate_weekly_report(self, language: str) -> Dict[str, str]:
		language = _normalize_language(language)
		date_tag = time.strftime("%Y-%m-%d")
		generated_at = time.strftime("%Y-%m-%dT%H:%M:%S")

		hotspot = self.generator.generate("hotspot", language)
		weak_signal = self.generator.generate("weak_signal", language)

		output_path = self.storage.get_output_path(language, date_tag)
		self.pdf_builder.build_unified_pdf(language, hotspot, weak_signal, output_path, generated_at)
		self.storage.update_metadata(language, output_path, generated_at)

		return {
			"path": str(output_path),
			"generated_at": generated_at,
			"sections": ["hotspot", "weak_signal"],
		}

	def generate_all_languages(self) -> Dict[str, Dict[str, str]]:
		fr_report = self.generate_weekly_report("fr")
		en_report = self.generate_weekly_report("en")
		return {"fr": fr_report, "en": en_report}

	def get_latest_report(self, language: str) -> Optional[Dict[str, str]]:
		return self.storage.get_latest(language)


_service_instance: Optional[WeeklyReportService] = None


def _should_log_metadata() -> bool:
	value = os.getenv(LOG_METADATA_ENV, "").strip().lower()
	return value in {"1", "true", "yes", "on"}


def get_weekly_report_service() -> WeeklyReportService:
	global _service_instance
	if _service_instance is None:
		generator = LangGraphReportGenerator()
		pdf_builder = ReportLabPDFBuilder()
		storage = FileSystemReportStorage(log_metadata=_should_log_metadata())
		_service_instance = WeeklyReportService(generator, pdf_builder, storage)
	return _service_instance


def generate_weekly_report(language: str) -> Dict[str, str]:
	return get_weekly_report_service().generate_weekly_report(language)


def generate_all_weekly_reports() -> Dict[str, Dict[str, str]]:
	return get_weekly_report_service().generate_all_languages()


def get_last_weekly_report(language: str) -> Optional[Dict[str, str]]:
	return get_weekly_report_service().get_latest_report(language)


def get_all_last_weekly_reports() -> Dict[str, Optional[Dict[str, str]]]:
	return {
		"fr": get_last_weekly_report("fr"),
		"en": get_last_weekly_report("en"),
	}


def hebdo_weekly_report_generator() -> None:
	generate_all_weekly_reports()


def hebdo_hotspots_briefing_generator(language: str = "francais") -> None:
	_ = language
	generate_all_weekly_reports()


def hebdo_weak_signals_briefing_generator() -> None:
	generate_all_weekly_reports()


def get_last_hotspot_report() -> Optional[str]:
	report = get_last_weekly_report("fr")
	return report.get("path") if report else None


def get_last_hotspot_report_by_language(language: str) -> Optional[str]:
	report = get_last_weekly_report(language)
	return report.get("path") if report else None