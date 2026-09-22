import Image from 'next/image';

export function Logo() {
  return (
    <>
      <Image
        src="/cachet.png"
        alt="Cachet"
        width={108}
        height={36}
        priority
        className="h-7 w-auto dark:invert-0 invert"
      />
    </>
  );
}
