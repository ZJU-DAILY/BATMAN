import "./globals.css";

import type { Metadata } from "next";

import { AppShell } from "@/components/app-shell";


export const metadata: Metadata = {
  title: "BATMAN",
  description: "Automatic data preparation demo system",
  icons: {
    icon: "/icon.svg"
  }
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <AppShell>{children}</AppShell>
      </body>
    </html>
  );
}
