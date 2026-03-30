"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { ReactNode } from "react";


function NavIcon({ name, active }: { name: string; active: boolean }) {
  const stroke = active ? "#2563eb" : "#64748b";
  const commonProps = {
    width: 20,
    height: 20,
    viewBox: "0 0 24 24",
    fill: "none",
    "aria-hidden": true as const,
    className: "h-5 w-5 shrink-0"
  };

  if (name === "Home") {
    return (
      <svg {...commonProps}>
        <path d="M4 11.5 12 5l8 6.5" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M6.5 10.5V19h11v-8.5" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }

  if (name === "Input") {
    return (
      <svg {...commonProps}>
        <path d="M6 8h12M6 12h8M6 16h12" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" />
        <path d="M18 6v12M18 6l-3 3M18 6l3 3" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }

  if (name === "Review") {
    return (
      <svg {...commonProps}>
        <path d="M2.5 12s3.5-6 9.5-6 9.5 6 9.5 6-3.5 6-9.5 6-9.5-6-9.5-6Z" stroke={stroke} strokeWidth="1.8" />
        <circle cx="12" cy="12" r="2.8" stroke={stroke} strokeWidth="1.8" />
      </svg>
    );
  }

  if (name === "Output") {
    return (
      <svg {...commonProps}>
        <path d="M12 4v10" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" />
        <path d="M8.5 10.5 12 14l3.5-3.5" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M5 18.5h14" stroke={stroke} strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    );
  }

  if (name === "Settings") {
    return (
      <svg {...commonProps}>
        <path
          d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 0 0 2.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 0 0 1.066 2.572c1.756.426 1.756 2.925 0 3.35a1.724 1.724 0 0 0-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 0 0-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 0 0-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 0 0-1.066-2.573c-1.756-.425-1.756-2.924 0-3.35a1.724 1.724 0 0 0 1.066-2.572c-.94-1.544.827-3.31 2.37-2.37.996.608 2.296.07 2.573-1.066Z"
          stroke={stroke}
          strokeWidth="1.8"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <circle cx="12" cy="12" r="3.1" stroke={stroke} strokeWidth="1.8" />
      </svg>
    );
  }

  return <div aria-hidden="true" className="h-5 w-5 rounded-full bg-slate-300" />;
}


const NAV_ITEMS = [
  { href: "/", label: "Home" },
  { href: "/input", label: "Input" },
  { href: "/review", label: "Review" },
  { href: "/output", label: "Output" },
  { href: "/settings", label: "Settings" }
];


export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="flex h-[max(100vh,820px)] w-[max(100vw,1280px)] bg-[#f8f9fb]">
      <aside className="flex h-full w-[220px] shrink-0 flex-col border-r border-slate-200 bg-white">
        <div className="px-6 pt-7">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-blue-100 px-1 text-[0.42rem] font-bold leading-none tracking-[0.08em] text-blue-700">
              BATMAN
            </div>
            <div className="text-[2rem] font-semibold tracking-tight text-slate-900">BATMAN</div>
          </div>
        </div>

        <nav className="mt-8 flex-1 space-y-1 px-3">
          {NAV_ITEMS.map((item) => {
            const active = pathname === item.href;
            return (
              <Link
                key={item.href}
                href={item.href}
                className={`flex items-center gap-3 rounded-2xl px-4 py-3 text-[1.05rem] font-medium transition ${
                  active ? "bg-blue-100 text-blue-700" : "text-slate-600 hover:bg-slate-100 hover:text-slate-900"
                }`}
              >
                <span className="flex h-6 w-6 shrink-0 items-center justify-center">
                  <NavIcon name={item.label} active={active} />
                </span>
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>
      </aside>

      <main className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden px-8 py-5">
        {children}
      </main>
    </div>
  );
}
