// The product mark followed by the wordmark, used wherever a page header
// carries the brand. The mark is served from public/mark.svg (a copy of
// docs/logo/mark.svg) and is decorative: the wordmark already names the
// product, so the image has an empty alt.
export default function Brand() {
  return (
    <span className="brand">
      <img className="brand__mark" src="/mark.svg" alt="" width={24} height={24} />
      Continuo
    </span>
  );
}
