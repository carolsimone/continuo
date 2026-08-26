// The product mark followed by the wordmark, used wherever a page header
// carries the brand. It links to the dashboard, as a site logo conventionally
// does. The mark is served from public/mark-light.svg (a copy of
// docs/logo/mark-light.svg, the variant drawn for light backgrounds) and is
// decorative: the wordmark already names the product, so the image has an
// empty alt.
export default function Brand() {
  return (
    <a className="brand" href="/">
      <img className="brand__mark" src="/mark-light.svg" alt="" width={28} height={28} />
      continuo
    </a>
  );
}
