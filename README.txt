962ElStore - نسخة ملف Python واحد للمرحلة الأولى

ارفع main.py و requirements.txt فقط إلى جذر مستودع GitHub.
Render Build Command: pip install -r requirements.txt
Render Start Command: python main.py

متغيرات Render المطلوبة: BOT_TOKEN, DATABASE_URL, ADMINS
اختياري: DB_SCHEMA_PROD=els962, DB_SCHEMA_TEST=els962_test, DB_POOL_MAX=5, STORE_NAME=962ElStore, LOG_LEVEL=INFO, PORT

لا ترفع أي أسرار إلى GitHub. هذه نسخة تأسيسية وليست المتجر الكامل. التشغيل ينشئ/يستخدم المخططين المخصصين فقط ولا يستهدف public. اختبر على Supabase قبل الإطلاق.
