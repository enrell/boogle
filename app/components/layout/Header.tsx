import { Logo } from "../ui/Logo";
import { SearchBar } from "../features/search/SearchBar";
import { ThemeToggle } from "../ui/ThemeToggle";
import { LanguageSwitcher } from "../ui/LanguageSwitcher";

interface HeaderProps {
  query?: string;
}

export function Header({ query }: HeaderProps) {
  return (
    <header className="sticky top-0 z-30 bg-white dark:bg-boogle-dark border-b border-gray-200 dark:border-white/5">
      <div className="max-w-[1600px] mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16 gap-4">
          {/* Logo */}
          <div className="flex-shrink-0">
            <Logo size="small" />
          </div>

          {/* Centered Search Bar */}
          <div className="flex-1 flex justify-center max-w-3xl mx-auto">
            <div className="w-full max-w-2xl">
              <SearchBar defaultValue={query} variant="header" />
            </div>
          </div>

          {/* Right Controls */}
          <div className="flex-shrink-0 flex items-center gap-3">
            <LanguageSwitcher />
            <ThemeToggle />
          </div>
        </div>
      </div>
    </header>
  );
}
