import Image from 'next/image';

/**
 * The header lockup.
 *
 * The wordmark in the logo is white with a transparent background, so on a light page it would be
 * invisible. Rather than ship a second asset, the mark sits on a dark chip in both themes — which
 * also keeps the gradient reading the same way regardless of what is behind it.
 */
export function Logo() {
  return (
    <span className="inline-flex items-center rounded-md bg-neutral-950 px-2.5 py-1.5">
      <Image
        src="/cachet-logo.png"
        alt="Cachet"
        width={108}
        height={36}
        priority
        className="h-5 w-auto"
      />
    </span>
  );
}
