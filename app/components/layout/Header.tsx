import { Logo } from "../ui/Logo";
import { SearchBar } from "../features/search/SearchBar";
import { ThemeToggle } from "../ui/ThemeToggle";
import { LanguageSwitcher } from "../ui/LanguageSwitcher";

interface HeaderProps {
  query?: string;
}

export function Header({ query }: HeaderProps) {
  return (
    <header className="flex flex-wrap md:flex-nowrap items-center justify-between md:justify-start p-3 md:p-4 border-b border-gray-200 dark:border-white/5 gap-y-3 md:gap-4 sticky top-0 bg-white dark:bg-boogle-dark z-30">
      <div className="flex-shrink-0 order-1">
        <Logo size="small" />
      </div>

      <div className="order-3 md:order-2 w-full md:w-auto md:flex-1 flex justify-center md:justify-start md:px-8">
        <div className="w-full max-w-none md:max-w-2xl">
          <SearchBar defaultValue={query} variant="header" />
        </div>
      </div>

      <div className="flex-shrink-0 flex items-center gap-2 order-2 md:order-3">
        <LanguageSwitcher />
        <ThemeToggle />
      </div>
    </header>
  );
}
