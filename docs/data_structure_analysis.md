# تحليل بنية البيانات (Data Structure Analysis)

هذا التقرير يستعرض أمثلة حقيقية لشكل البيانات داخل قاعدة البيانات (MongoDB) بعد معالجتها، ويوضح بدقة ما هي الحقول الإضافية (Metadata & Audit Trail) التي يقوم النظام بإرفاقها بكل نوع من السجلات.

---

## 1. سجل سليم تماماً (Valid Record)
هذا السجل كان مثالياً من البداية، ولم يحتج لأي تعديلات. تم حفظه في جدول `orders_validated`.

```json
{
  "_id": "64f9b8c3e4b0a1c2d3e4f5a6",
  "order_id": "طلب-100000",
  "customer_id": "عميل-0",
  "order_date": "2025-02-24T21:29:00",
  "status": "مؤكد",
  "total_amount": "769000.0",
  "currency": "YER",
  "payment_method": "محفظة إلكترونية",
  "payment_status": "تم الدفع",
  "quality_status": "VALID",
  "corrections": [],
  "metadata": {
    "run_id": "5b06b307cb2b46b881aa83d7bbd9db59",
    "processed_at": "2026-08-26T19:40:00Z",
    "source_file": "orders_huge_mixed_quality.csv",
    "engine_used": "pyspark"
  }
}
```
**ما الذي تم إرفاقه؟**
- `quality_status`: أخذ قيمة `VALID`.
- `corrections`: مصفوفة فارغة `[]` لأنه لم تحدث أي تصحيحات.
- `metadata`: بيانات تتبعية توضح متى تمت المعالجة، وما هو الملف المصدر، والمحرك المستخدم.

---

## 2. سجل فيه أخطاء وتمت معالجته (Corrected Record)
هذا السجل كان يحتوي على أخطاء (مثل أرقام عربية/فارسية أو فواصل آلاف أو كلمات بدلاً من أرقام)، وتدخلت قواعد الجودة لإصلاحه. تم حفظه في جدول `orders_validated`.

```json
{
  "_id": "64f9b8c3e4b0a1c2d3e4f5a7",
  "order_id": "طلب-100003",
  "customer_id": "عميل-3",
  "total_amount": "706000.0",
  "currency": "YER",
  "quality_status": "CORRECTED",
  "corrections": [
    {
      "field": "total_amount",
      "original_value": "٧٠٦٠٠٠٫٠",
      "corrected_value": "706000.0",
      "rule_code": "R1_NORMALIZE_DIGITS"
    }
  ],
  "metadata": {
    "run_id": "5b06b307cb2b46b881aa83d7bbd9db59",
    "processed_at": "2026-08-26T19:40:01Z",
    "source_file": "orders_huge_mixed_quality.csv",
    "engine_used": "pyspark"
  }
}
```
**ما الذي تم إرفاقه؟**
- `quality_status`: أخذ قيمة `CORRECTED`.
- `corrections`: (سجل التدقيق Audit Trail) ويحتوي بوضوح على الحقل الذي تم تعديله (`total_amount`)، كيف كانت القيمة الفاسدة (`٧٠٦٠٠٠٫٠`) وكيف أصبحت بعد التنظيف (`706000.0`)، واسم القاعدة التي نفذت ذلك (`R1`).
- `metadata`: بيانات التتبع كما في السجل السليم.

---

## 3. سجل فاسد وتم رفضه (Quarantine Record)
هذا السجل كان يفتقد لمعلومات جوهرية (مثل `order_id` مفقود أو بيانات غير قابلة للقراءة)، فتم رفضه وعزله في جدول `orders_quarantine`.

```json
{
  "_id": "64f9b8c3e4b0a1c2d3e4f5a8",
  "quarantine_reasons": [
    "MISSING_ORDER_ID"
  ],
  "metadata": {
    "run_id": "5b06b307cb2b46b881aa83d7bbd9db59",
    "quarantined_at": "2026-08-26T19:40:02Z",
    "source_file": "orders_huge_mixed_quality.csv",
    "engine_used": "pyspark"
  },
  "raw_record": {
    "order_id": "",
    "customer_id": "عميل-7",
    "total_amount": "26000.0",
    "status": "قيد الشحن"
  },
  "cleaned_draft": {
    "order_id": "",
    "customer_id": "عميل-7",
    "total_amount": "26000.0",
    "status": "قيد الشحن"
  },
  "corrections": []
}
```
**ما الذي تم إرفاقه بالسجل المرفوض؟**
- `quarantine_reasons`: مصفوفة توضح بدقة سبب الرفض (في هذا المثال: `MISSING_ORDER_ID`).
- `raw_record`: السجل الأصلي تماماً كما جاء من ملف الـ CSV دون أي مساس به، ليتمكن المهندس من دراسته.
- `cleaned_draft`: المسودة التي حاول النظام تنظيفها قبل أن يكتشف أنها فاسدة ولا يمكن إنقاذها.
- `corrections`: أي تصحيحات حاول النظام عملها قبل الرفض.
- `metadata`: بيانات التتبع (متى تم العزل).
