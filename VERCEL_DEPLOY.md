# 🚀 Деплой на Vercel

## Быстрый деплой

### 1. Установите Vercel CLI

```bash
npm install -g vercel
```

### 2. Войдите в Vercel

```bash
vercel login
```

### 3. Деплой проекта

```bash
cd C:\Users\Kamron\PycharmProjects\tour\TourVisor
vercel
```

Следуйте инструкциям:
- Set up and deploy? **Y**
- Which scope? **Ваш аккаунт**
- Link to existing project? **N**
- What's your project's name? **travel-with-alina-bot**
- In which directory is your code located? **./** (текущая папка)

### 4. Настройте переменные окружения

```bash
vercel env add OPENAI_API_KEY
vercel env add FACEBOOK_PAGE_ACCESS_TOKEN
vercel env add TELEGRAM_BOT_TOKEN
vercel env add TELEGRAM_GROUP_ID
vercel env add TOURVISOR_LOGIN
vercel env add TOURVISOR_PASSWORD
```

Для каждой переменной:
- Production? **Y**
- Preview? **Y**  
- Development? **Y**

### 5. Получите URL

После деплоя Vercel покажет URL:
```
https://travel-with-alina-bot.vercel.app
```

### 6. Настройте Facebook Webhook

Используйте URL из Vercel:
```
https://travel-with-alina-bot.vercel.app/webhook
```

Verify Token: `travel_with_alina_bot`

---

## Обновление деплоя

После изменений в коде:

```bash
git add .
git commit -m "Update bot"
git push

# И деплой
vercel --prod
```

---

## Логи на Vercel

Просмотр логов:
```bash
vercel logs
```

Или в веб-интерфейсе:
https://vercel.com/dashboard

---

## Особенности Vercel

✅ **Что работает:**
- Serverless функции
- Автоматический HTTPS
- Глобальный CDN
- Автомасштабирование

⚠️ **Ограничения:**
- Read-only файловая система (нельзя писать файлы)
- Timeout: 10 секунд (Hobby), 60 секунд (Pro)
- Память: 1024 MB (Hobby), 3008 MB (Pro)

💡 **Решения:**
- Логи → stdout (не в файлы)
- Кэш → in-memory (не на диск)
- Долгие операции → background job services

---

## Проблемы и решения

**Проблема:** `OSError: Read-only file system`
**Решение:** Код исправлен - логи идут в stdout, а не в файлы

**Проблема:** Timeout
**Решение:** Увеличить план или оптимизировать GPT запросы

**Проблема:** Холодный старт
**Решение:** Vercel держит функции "теплыми" на платных планах

---

## Мониторинг

Vercel Dashboard показывает:
- Количество запросов
- Время ответа
- Ошибки
- Использование ресурсов

---

✅ Готово! Бот работает на Vercel!

