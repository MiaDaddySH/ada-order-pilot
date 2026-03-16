import json
import re
from dataclasses import dataclass
from typing import Any

from openai import OpenAI
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    llm_base_url: str | None = None
    llm_api_key: str | None = None
    llm_model: str = "gpt-4o-mini"
    parse_mode: str = "llm_only"
    db_path: str = "data/ada_order.db"
    recipient_template_path: str = "templates/收件人模板.xlsx"
    order_template_path: str = "templates/订单导入模板.xlsx"
    export_dir: str = "exports"
    sender_name: str = "Jing Zhu"
    sender_phone: str = "15201069795"
    sender_street: str = "Emilienstraße"
    sender_house_no: str = "24"
    sender_postcode: str = "70563"
    sender_city: str = "Stuttgart"
    sender_country_code: str = "DE"
    recipient_country_code: str = "CN"
    channel_code: str = "2G"
    goods_purpose: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


class LLMParseResult(BaseModel):
    recipient: dict[str, Any]
    products: list[dict[str, Any]]
    confidence: float
    needs_review: bool
    parse_source: str


@dataclass
class LLMOrderParser:
    settings: Settings

    def parse_order(self, text: str) -> LLMParseResult:
        mode = self.settings.parse_mode.lower().strip()
        if mode == "fallback":
            return self._fallback_parse(text)
        if not self.settings.llm_api_key:
            raise RuntimeError("LLM_API_KEY 未配置，当前模式不允许规则兜底")
        try:
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
            payload = json.loads(content)
            if isinstance(payload, dict):
                payload["parse_source"] = "llm"
            return LLMParseResult.model_validate(payload)
        except Exception as exc:
            if mode == "llm_with_fallback":
                return self._fallback_parse(text)
            raise RuntimeError("LLM 解析失败，当前模式不允许规则兜底") from exc

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
                    "simple_code": "string|null",
                }
            ],
            "confidence": "float 0~1",
            "needs_review": "bool",
        }
        return (
            "从下面文本提取收件人和商品信息，按给定 schema 返回。"
            f"schema={json.dumps(schema, ensure_ascii=False)};"
            "如果文本里出现商品简易代码，放入simple_code字段。"
            "如果信息不确定，needs_review=true，confidence降低。"
            f"文本={text}"
        )

    def _fallback_parse(self, text: str) -> LLMParseResult:
        normalized_text = text.replace("➕", "+").replace("＋", "+")
        cleaned_text = self._strip_product_segment(text)
        phone = self._extract_phone(cleaned_text)
        recipient_name = self._extract_name(cleaned_text, phone)
        address_source = self._extract_address_source(cleaned_text, phone)
        province, city, district, address_detail = self._split_address(address_source)
        quantity_match = re.search(r"(\d+)\s*(盒|罐|袋|听)", normalized_text)
        quantity = int(quantity_match.group(1)) if quantity_match else 1
        unit = quantity_match.group(2) if quantity_match else "盒"
        stage_match = re.search(r"(pre|PRE|\d+\+?段|\d+\+)", normalized_text)
        stage = stage_match.group(1) if stage_match else None
        simple_code_match = re.search(
            r"(?<![0-9A-Za-z])([A-Za-z]{1,6}\d\+?|[0-9]{10,})(?![0-9A-Za-z])",
            normalized_text,
        )
        simple_code = simple_code_match.group(1) if simple_code_match else None
        return LLMParseResult(
            recipient={
                "name": recipient_name,
                "phone": phone,
                "province": province,
                "city": city,
                "district": district,
                "address_detail": address_detail,
                "raw_address": address_source,
                "postcode": None,
            },
            products=[
                {
                    "brand": None,
                    "product_name": text,
                    "stage": stage,
                    "quantity": quantity,
                    "unit": unit,
                    "simple_code": simple_code,
                }
            ],
            confidence=0.2,
            needs_review=True,
            parse_source="fallback",
        )

    def _strip_product_segment(self, text: str) -> str:
        head = re.split(r"[（(]", text, maxsplit=1)[0]
        return head.strip()

    def _extract_phone(self, text: str) -> str:
        matched = re.search(r"1[3-9]\d{9}", text)
        if matched:
            return matched.group(0)
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits[-11:] if len(digits) >= 11 else "00000000000"

    def _extract_name(self, text: str, phone: str) -> str:
        normalized = text.replace("\r\n", "\n")
        line_with_phone = ""
        for line in normalized.split("\n"):
            if phone in line:
                line_with_phone = line
                break
        candidate = line_with_phone if line_with_phone else normalized
        before_phone = candidate.split(phone, maxsplit=1)[0]
        explicit_name = re.search(r"([\u4e00-\u9fff]{2,6})\s*[：: ]*\s*(?:电话)?\s*$", before_phone)
        if explicit_name:
            return explicit_name.group(1)
        candidate = candidate.replace(phone, " ")
        parts = [part.strip() for part in re.split(r"[，,;；]", candidate) if part.strip()]
        if parts:
            candidate = parts[-1]
        name_matches = re.findall(r"[\u4e00-\u9fff]{2,6}", candidate)
        if name_matches:
            return str(name_matches[0])
        return "待确认"

    def _extract_address_source(self, text: str, phone: str) -> str:
        normalized = text.replace("\r\n", "\n")
        lines = [line.strip() for line in normalized.split("\n") if line.strip()]
        for line in lines:
            if phone in line:
                continue
            if any(token in line for token in ("省", "市", "区", "县", "街道", "镇", "路", "号")):
                return self._trim_to_address_start(line)
        without_phone = normalized.replace(phone, " ")
        without_phone = self._trim_to_address_start(without_phone)
        parts = [part.strip() for part in re.split(r"[，,;；]", without_phone) if part.strip()]
        if parts:
            return parts[0]
        return without_phone.strip()

    def _split_address(self, address: str) -> tuple[str | None, str | None, str | None, str]:
        working = self._trim_to_address_start(address.strip())
        province = None
        city = None
        district = None

        province_match = re.match(r"^(.+?(?:省|自治区|特别行政区|市))", working)
        if province_match:
            province = province_match.group(1)
            working = working[len(province) :].strip()

        city_match = re.match(r"^(.+?市)", working)
        if city_match is None:
            city_match = re.match(r"^(.+?(?:州|盟|地区))", working)
        if city_match:
            city = city_match.group(1)
            working = working[len(city) :].strip()
        elif province and province.endswith("市"):
            city = province

        district_match = re.match(r"^(.+?(?:区|县|旗))", working)
        if district_match is None:
            district_match = re.match(r"^(.+?市)", working)
        if district_match:
            district = district_match.group(1)
            working = working[len(district) :].strip()

        address_detail = working if working else address.strip()
        return province, city, district, address_detail

    def _trim_to_address_start(self, value: str) -> str:
        trimmed = re.sub(r"^\s*地址[:：]?\s*", "", value).strip()
        matched = re.search(r"([\u4e00-\u9fff]{2,}(?:省|自治区|特别行政区)|[\u4e00-\u9fff]{2,}市)", trimmed)
        if matched:
            return trimmed[matched.start() :].strip()
        return trimmed
