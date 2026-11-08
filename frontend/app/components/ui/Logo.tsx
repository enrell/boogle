import { Link } from "react-router";

interface LogoProps {
  size?: "small" | "large";
}

export function Logo({ size = "large" }: LogoProps) {
  const textSize = size === "large" ? "text-6xl" : "text-2xl";
  
  return (
    <Link to="/" className={`${textSize} font-bold text-gray-800 dark:text-white tracking-tighter select-none transition-colors duration-300`}>
      Boogle
    </Link>
  );
}
