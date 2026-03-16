import json
from dataclasses import dataclass
from typing import Any

from openai import OpenAI
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    llm_base_url: str | None = None
    llm_api_key: str | None = None
    llm_model: str = "gpt-4o-mini"
    db_path: str = "data/ada_order.db"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class LLMParseResult(BaseModel):
    recipient: dict[str, Any]
    products: list[dict[str, Any]]
    confidence: float
    needs_review: bool


@dataclass
class LLMOrderParser:
    settings: Settings

    def parse_order(self, text: str) -> LLMParseResult:
        if not self.settings.llm_api_key:
            return self._fallback_parse(text)
        client = OpenAI(
            api_key=self.settings.llm_api_key,
            base_url=self.settings.llm_base_url or None,
        )
        prompt = self._build_prompt(text)
        response = client.chat.completions.create(
            model=self.settings.llm_model,
            messages=[
                {
                    "role": "system",
                    "content": "你是订单解析器，只输出严格 JSON，不要额外文本。",
                },
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        content = response.choices[0].message.content or "{}"
        return LLMParseResult.model_validate(json.loads(content))

    def _build_prompt(self, text: str) -> str:
        schema = {
            "recipient": {
                "name": "string",
                "phone": "string",
                "province": "string|null",
                "city": "string|null",
                "district": "string|null",
                "address_detail": "string",
                "raw_address": "string",
                "postcode": "string|null",
            },
            "products": [
                {
                    "brand": "string|null",
                    "product_name": "string",
                    "stage": "string|null",
                    "quantity": "int >=1",
                    "unit": "string",
                }
            ],
            "confidence": "float 0~1",
            "needs_review": "bool",
        }
        return (
            "从下面文本提取收件人和商品信息，按给定 schema 返回。"
            f"schema={json.dumps(schema, ensure_ascii=False)};"
            "如果信息不确定，needs_review=true，confidence降低。"
            f"文本={text}"
        )

    def _fallback_parse(self, text: str) -> LLMParseResult:
        digits = "".join(ch for ch in text if ch.isdigit())
        phone = digits[-11:] if len(digits) >= 11 else "00000000000"
        return LLMParseResult(
            recipient={
                "name": "待确认",
                "phone": phone,
                "province": None,
                "city": None,
                "district": None,
                "address_detail": text,
                "raw_address": text,
                "postcode": None,
            },
            products=[
                {
                    "brand": None,
                    "product_name": "待确认",
                    "stage": None,
                    "quantity": 1,
                    "unit": "盒",
                }
            ],
            confidence=0.2,
            needs_review=True,
        )
