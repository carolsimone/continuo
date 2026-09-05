import { buildServiceColors } from './service-helpers';

// ServiceTiles shows the set of service images a run executes with: one tile
// per service, sorted by name so a service sits in the same place on every
// release, carrying the accent buildServiceColors assigns it within this set,
// its name, and its image tag. The changed service — the one whose image is
// new in this run — is marked, and the sub-line says so, naming the kind of
// run the page shows (subject: "release" unless the page says otherwise) and
// where every other image was carried over from (carriedFrom: "prod" for a
// candidate; the verified release for a verification run, whose set is that
// release's with the fixed service swapped in). Renders nothing when no image
// has been recorded yet.
export default function ServiceTiles({ imageTags, changedService, subject = 'release', carriedFrom = 'prod' }: {
  imageTags: Record<string, string>;
  changedService?: string;
  subject?: string;
  carriedFrom?: string;
}) {
  const names = Object.keys(imageTags).sort();
  if (names.length === 0) return null;
  const colors = buildServiceColors(names);
  const changed = changedService && names.includes(changedService) ? changedService : '';
  const carried = names.length - 1;
  const sub = changed
    ? `${changed} is new in this ${subject}${carried > 0 ? ` · ${carried} carried over from ${carriedFrom}` : ''}`
    : '';
  return (
    <>
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Services</span>
          <span className="section-header__count">{names.length}</span>
        </div>
        {sub && <div className="section-header__sub">{sub}</div>}
      </div>
      <div className="service-tile-grid">
        {names.map(name => (
          <div key={name} className={`service-tile${name === changed ? ' service-tile--changed' : ''}`}>
            <div className="service-tile__head">
              <span className="nodes-group-dot" style={{ background: colors.get(name) }} />
              <span className="service-tile__name">{name}</span>
              {name === changed && <span className="pill-sm pill-sm--changed">changed</span>}
            </div>
            <div className="service-tile__tag">{imageTags[name]}</div>
          </div>
        ))}
      </div>
    </>
  );
}
