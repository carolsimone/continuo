import { useNavigate } from 'react-router';

interface Props {
  // Dormant nodes of the live release: nodes continuo builds that have no
  // schedule tag, so no cron or schedule run includes them.
  count: number;
}

// The Topology tab's tile for the dormant nodes, after the schedule tiles: it
// opens the Nodes tab filtered to them.
export default function DormantTile({ count }: Props) {
  const navigate = useNavigate();
  return (
    <button type="button" className="snapshot-tile" onClick={() => navigate('/?tab=nodes&dormant=1')}>
      <div className="snapshot-tile__name">Dormant</div>
      <div className="snapshot-tile__meta">
        {count} dormant {count === 1 ? 'node' : 'nodes'} · run only by hand
      </div>
    </button>
  );
}
