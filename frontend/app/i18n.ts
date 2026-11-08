import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import LanguageDetector from "i18next-browser-languagedetector";

// Import translations directly
import en from "./locales/en/translation.json";
import pt from "./locales/pt/translation.json";
import ptBR from "./locales/pt-BR/translation.json";
import fr from "./locales/fr/translation.json";
import es from "./locales/es/translation.json";
import ja from "./locales/ja/translation.json";

export const resources = {
    en: { translation: en },
    pt: { translation: pt },
    "pt-BR": { translation: ptBR },
    fr: { translation: fr },
    es: { translation: es },
    ja: { translation: ja },
};

const i18nInstance = i18n.use(initReactI18next);

// Only use browser language detector on the client side
if (typeof window !== "undefined") {
    i18nInstance.use(LanguageDetector);
}

i18nInstance.init({
    resources,
    fallbackLng: "en",
    supportedLngs: ["en", "pt", "pt-BR", "fr", "ja", "es"],
    interpolation: {
        escapeValue: false,
    },
    detection: {
        order: ["querystring", "localStorage", "cookie", "navigator"],
        caches: ["localStorage", "cookie"],
    },
});

export default i18n;
