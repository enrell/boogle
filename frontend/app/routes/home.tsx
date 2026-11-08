import type { Route } from "./+types/home";
import { Logo } from "../components/ui/Logo";
import { SearchBar } from "../components/features/search/SearchBar";
import { Footer } from "../components/layout/Footer";
import { ThemeToggle } from "../components/ui/ThemeToggle";
import { LanguageSwitcher } from "../components/ui/LanguageSwitcher";

export function meta({ }: Route.MetaArgs) {
  return [
    { title: "Boogle" },
    { name: "description", content: "Boogle - Open Source Search Engine for Free Books" },
  ];
}

export default function Home() {
  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-white dark:bg-boogle-dark transition-colors duration-300">
      <div className="absolute top-4 right-4 flex items-center gap-2">
        <LanguageSwitcher />
        <ThemeToggle />
      </div>
      <div className="w-full max-w-2xl px-4 flex flex-col items-center gap-8">
        <Logo />
        <SearchBar variant="centered" />
      </div>
      <div className="absolute bottom-0 w-full">
        <Footer />
      </div>
    </div>
  );
}
