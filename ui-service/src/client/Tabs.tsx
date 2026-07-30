import { useSearchParams } from 'react-router';

export type TabSpec = {
  slug: string;
  label: string;
  count?: number;
};

type Props = {
  tabs: TabSpec[];
  param: string;
  defaultSlug: string;
  variant?: 'page' | 'panel';
};

export function useActiveTab(param: string, defaultSlug: string, knownSlugs: string[]): string {
  const [searchParams] = useSearchParams();
  const raw = searchParams.get(param);
  if (raw && knownSlugs.includes(raw)) return raw;
  return defaultSlug;
}

export default function Tabs({ tabs, param, defaultSlug, variant = 'page' }: Props) {
  const [searchParams, setSearchParams] = useSearchParams();
  const knownSlugs = tabs.map(t => t.slug);
  const active = useActiveTab(param, defaultSlug, knownSlugs);

  const select = (slug: string) => {
    const next = new URLSearchParams(searchParams);
    if (slug === defaultSlug) next.delete(param);
    else next.set(param, slug);
    setSearchParams(next, { replace: false });
  };

  const navClass = ['tabs', variant === 'panel' ? 'tabs--panel' : ''].filter(Boolean).join(' ');

  return (
    <nav className={navClass} role="tablist">
      {tabs.map(t => {
        const isActive = t.slug === active;
        const tabClass = ['tabs__tab', isActive ? 'tabs__tab--active' : ''].filter(Boolean).join(' ');
        return (
          <a
            key={t.slug}
            role="tab"
            aria-selected={isActive}
            className={tabClass}
            href={t.slug === defaultSlug ? '#' : `?${param}=${t.slug}`}
            onClick={e => { e.preventDefault(); select(t.slug); }}
          >
            {t.label}
            {typeof t.count === 'number' && (
              <span className="tabs__count">{t.count}</span>
            )}
          </a>
        );
      })}
    </nav>
  );
}
