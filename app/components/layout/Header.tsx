import { Logo } from "../ui/Logo";
import { SearchBar } from "../features/search/SearchBar";
import { ThemeToggle } from "../ui/ThemeToggle";

interface HeaderProps {
  query?: string;
}

export function Header({ query }: HeaderProps) {
  return (
    <header className="flex items-center p-4 border-b border-gray-200 dark:border-gray-600 gap-6 sticky top-0 bg-white dark:bg-boogle-dark z-10">
      <Logo size="small" />
      <div className="flex-1 max-w-2xl">
        <SearchBar defaultValue={query} variant="header" />
      </div>
      <ThemeToggle />
    </header>
  );
}
