from __future__ import annotations

from app.models import CandidatePipeline, Session


class SuggestionService:
    def build(self, session: Session, candidate: CandidatePipeline) -> list[str]:
        suggestions: list[str] = []
        warnings = candidate.validation_summary.warnings
        required_fields = {field.name for field in session.target_schema if field.required}
        matched_fields = {check.field_name for check in candidate.validation_summary.field_checks if check.status == "Matched"}

        if "Shop_id" in required_fields and "Shop_id" not in matched_fields:
            suggestions.append("Use Shop_id")
        if "Date" in required_fields and "Date" not in matched_fields:
            suggestions.append("Normalize Date")
        if "Product_category" in required_fields and "Product_category" not in matched_fields:
            suggestions.append("Keep product rows")
        if "Total_store_sales" in {field.name for field in session.target_schema}:
            suggestions.append("Check daily total")
        if any("type" in warning.lower() for warning in warnings):
            suggestions.append("Review field types")
        if any("missing" in warning.lower() for warning in warnings):
            suggestions.append("Check missing fields")

        unique: list[str] = []
        for suggestion in suggestions:
            if suggestion not in unique:
                unique.append(suggestion)
        return unique[:4]


suggestion_service = SuggestionService()
