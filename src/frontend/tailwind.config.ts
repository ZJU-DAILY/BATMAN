import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}"
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          50: "#eef4ff",
          100: "#dce9ff",
          200: "#bfd5ff",
          300: "#8eb7ff",
          400: "#5d96ff",
          500: "#3170ff",
          600: "#215fe6",
          700: "#1e4bb4",
          900: "#102243"
        }
      },
      boxShadow: {
        card: "0 20px 60px rgba(15, 32, 71, 0.08)"
      }
    }
  },
  plugins: []
};

export default config;
