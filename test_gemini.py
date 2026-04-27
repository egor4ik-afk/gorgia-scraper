# #!/usr/bin/env python3
# """
# Тест Gemini генерации описаний
# Запуск: GEMINI_API_KEY=ваш_ключ python test_gemini.py
# """
# import os
# import re
# import json

# GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# def generate_descriptions(name_ru, name_en, name_ka, category_ru, sub_category_ru):
#     if not GEMINI_API_KEY:
#         print("❌ GEMINI_API_KEY не задан")
#         return {}

#     import urllib.request
#     name = name_ru or name_en or name_ka
#     cat  = f"{category_ru} / {sub_category_ru}" if sub_category_ru else category_ru

#     prompt = f"""You are a product copywriter for an online store in Georgia (country).
# Write a short, natural product description (2-3 sentences, max 300 chars each) for:

# Product: {name}
# Category: {cat}

# Return ONLY a valid JSON object with exactly these keys:
# {{
#   "ru": "описание на русском",
#   "en": "description in english",
#   "ka": "აღწერა ქართულად"
# }}

# No markdown, no extra text, just the JSON."""

#     try:
#         url  = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
#         body = json.dumps({"contents": [{"parts": [{"text": prompt}]}]}).encode()
#         req  = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
#         with urllib.request.urlopen(req, timeout=30) as resp:
#             data = json.loads(resp.read())
#         text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
#         print(f"\n📝 Сырой ответ Gemini:\n{text}\n")
#         text = re.sub(r"```json\s*|\s*```", "", text).strip()
#         parsed = json.loads(text)
#         return {
#             "ru": str(parsed.get("ru", ""))[:500],
#             "en": str(parsed.get("en", ""))[:500],
#             "ka": str(parsed.get("ka", ""))[:500],
#         }
#     except Exception as e:
#         print(f"❌ Ошибка: {e}")
#         return {}


# if __name__ == "__main__":
#     print("🧪 Тест Gemini описаний\n")
#     result = generate_descriptions(
#         name_ru="Настольная лампа цвет медь БАРОМЕТР",
#         name_en="Table lamp copper color BAROMETER",
#         name_ka="მაგიდის სანათი სპილენძის ფერი BAROMETER",
#         category_ru="IKEA",
#         sub_category_ru="Освещение",
#     )
#     if result:
#         print("✅ Результат:")
#         print(f"  RU: {result['ru']}")
#         print(f"  EN: {result['en']}")
#         print(f"  KA: {result['ka']}")
#     else:
#         print("❌ Описание не сгенерировано")