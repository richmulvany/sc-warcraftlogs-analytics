import { useEffect, useState } from 'react'

export function useActiveSection(sections: readonly { id: string }[]): string {
  const [active, setActive] = useState(sections[0]?.id ?? '')
  useEffect(() => {
    function recompute() {
      // 130px clears the header (title + nav strip) plus a small buffer
      const offset = 130
      let current = sections[0]?.id ?? ''
      for (const s of sections) {
        const el = document.getElementById(s.id)
        if (!el) continue
        if (el.getBoundingClientRect().top - offset <= 0) current = s.id
        else break
      }
      setActive(current)
    }
    recompute()
    document.addEventListener('scroll', recompute, { passive: true, capture: true })
    window.addEventListener('resize', recompute)
    return () => {
      document.removeEventListener('scroll', recompute, { capture: true })
      window.removeEventListener('resize', recompute)
    }
  }, [sections])
  return active
}

export function SectionNav({
  sections,
  activeId,
}: {
  sections: readonly { id: string; label: string }[]
  activeId: string
}) {
  return (
    <nav className="flex items-center gap-1.5 overflow-x-auto px-8 py-2 border-t border-ctp-surface0/60">
      {sections.map(s => (
        <a
          key={s.id}
          href={`#${s.id}`}
          className={
            'px-2.5 py-1 rounded-lg text-[11px] 2xl:text-xs font-mono whitespace-nowrap transition-colors ' +
            (activeId === s.id
              ? 'bg-ctp-mauve/15 text-ctp-mauve border border-ctp-mauve/30'
              : 'text-ctp-overlay1 hover:text-ctp-text border border-transparent hover:bg-ctp-surface0/60')
          }
        >
          {s.label}
        </a>
      ))}
    </nav>
  )
}
