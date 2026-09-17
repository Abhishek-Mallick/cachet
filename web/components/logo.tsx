import Image from 'next/image';

/**
 * The header lockup.
 *
 * Two assets, one per theme. The wordmark is white in `cachet-logo.png` and near-black in
 * `cachet-logo-on-light.png`; the mark is the same teal gradient in both, because it reads on either
 * ground. Both are rendered and one is hidden by CSS rather than picked in JavaScript, so the right
 * one is in the server-rendered HTML and there is no flash of the wrong lockup on load.
 */
export function Logo() {
  return (
    <>
      <Image
        src="/cachet-logo-on-light.png"
        alt="Cachet"
        width={108}
        height={36}
        priority
        className="h-5 w-auto dark:hidden"
      />
      <Image
        src="/cachet-logo.png"
        alt=""
        aria-hidden
        width={108}
        height={36}
        priority
        className="hidden h-5 w-auto dark:block"
      />
    </>
  );
}
