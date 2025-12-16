import { useTranslation } from "react-i18next";

export function Footer() {
  const { t } = useTranslation();
  return (
    <footer className="w-full p-4 bg-gray-100 dark:bg-boogle-dark border-t border-gray-200 dark:border-white/5 text-center text-gray-600 dark:text-gray-400 text-sm mt-auto transition-colors duration-300">
      <div className="flex flex-col md:flex-row justify-between items-center max-w-6xl mx-auto">
        <p>{t('footer_text')}</p>
        <div className="flex gap-4 mt-2 md:mt-0">
          <a href="#" className="hover:underline">{t('about')}</a>
          <a href="#" className="hover:underline">{t('privacy')}</a>
          <a href="#" className="hover:underline">{t('terms')}</a>
        </div>
      </div>
    </footer>
  );
}
