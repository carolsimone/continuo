import { ReleaseTransition } from './types';
import { releasePillClass, upcomingStages } from './release-helpers';

// PipelineTimeline shows a run's path through the pipeline: one step per
// recorded transition, coloured like the run's status pill and stamped with
// when it happened, followed — while the run is still in flight — by the
// stages it has not reached yet, ghosted and undated.
export default function PipelineTimeline({ transitions }: { transitions: ReleaseTransition[] }) {
  return (
    <>
      <div className="section-header">
        <div className="section-header__main">
          <span className="section-header__title">Timeline</span>
        </div>
      </div>
      <ol className="release-timeline">
        {transitions.map((t, i) => (
          <li key={`${i}-${t.to}`} className="release-timeline__step">
            <span className="release-timeline__connector" aria-hidden="true" />
            <div className="release-timeline__body">
              <span className={`pill-sm ${releasePillClass(t.to).replace('pill--', 'pill-sm--')}`}>{t.to}</span>
              <span className="release-timeline__at">{t.at.slice(0, 19).replace('T', ' ')}</span>
            </div>
          </li>
        ))}
        {upcomingStages(transitions).map(stage => (
          <li key={stage} className="release-timeline__step release-timeline__step--upcoming">
            <span className="release-timeline__connector" aria-hidden="true" />
            <div className="release-timeline__body">
              <span className="pill-sm">{stage}</span>
              <span className="release-timeline__at"> </span>
            </div>
          </li>
        ))}
      </ol>
    </>
  );
}
