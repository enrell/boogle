import { useTranslation } from "react-i18next";
import { IoLanguage } from "react-icons/io5";
import { useState, useRef, useEffect } from "react";

export function LanguageSwitcher() {
    const { i18n } = useTranslation();
    const [isOpen, setIsOpen] = useState(false);
    const dropdownRef = useRef<HTMLDivElement>(null);

    const changeLanguage = (lng: string) => {
        i18n.changeLanguage(lng);
        setIsOpen(false);
    };

    // Close dropdown when clicking outside
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };

        if (isOpen) {
            document.addEventListener("mousedown", handleClickOutside);
        }

        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, [isOpen]);

    const languages = [
        { code: "en", label: "English", flag: "🇺🇸" },
        { code: "pt", label: "Português", flag: "🇵🇹" },
        { code: "pt-BR", label: "Português (BR)", flag: "🇧🇷" },
        { code: "fr", label: "Français", flag: "🇫🇷" },
        { code: "es", label: "Español", flag: "🇪🇸" },
        { code: "ja", label: "日本語", flag: "🇯🇵" },
    ];

    return (
        <div className="relative" ref={dropdownRef}>
            <button
                onClick={() => setIsOpen(!isOpen)}
                className="p-2 rounded-full hover:bg-gray-100 dark:hover:bg-white/5 text-gray-600 dark:text-gray-400 transition-colors cursor-pointer"
                aria-label="Select language"
                aria-expanded={isOpen}
            >
                <IoLanguage className="w-5 h-5" />
            </button>

            {isOpen && (
                <div className="absolute right-0 mt-2 w-48 bg-white dark:bg-boogle-dark border border-gray-200 dark:border-white/10 rounded-lg shadow-lg z-50 animate-fade-in">
                    <div className="py-1">
                        {languages.map((lang) => (
                            <button
                                key={lang.code}
                                onClick={() => changeLanguage(lang.code)}
                                className={`w-full text-left px-4 py-3 md:py-2 text-sm hover:bg-gray-50 dark:hover:bg-white/5 transition-colors cursor-pointer flex items-center ${i18n.language === lang.code
                                        ? "text-blue-600 dark:text-blue-400 font-medium"
                                        : "text-gray-700 dark:text-gray-300"
                                    }`}
                            >
                                <span className="mr-3 text-base">{lang.flag}</span>
                                {lang.label}
                            </button>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
