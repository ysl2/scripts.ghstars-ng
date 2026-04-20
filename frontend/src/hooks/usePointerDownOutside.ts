import { useEffect, useEffectEvent, type RefObject } from 'react'

export default function usePointerDownOutside<TElement extends HTMLElement>(
  ref: RefObject<TElement | null>,
  onOutside: (event: PointerEvent) => void,
  enabled = true,
) {
  const handleOutside = useEffectEvent((event: PointerEvent) => {
    const target = event.target
    if (!(target instanceof Node)) return
    if (ref.current?.contains(target)) return
    onOutside(event)
  })

  useEffect(() => {
    if (!enabled) return

    function handlePointerDown(event: PointerEvent) {
      handleOutside(event)
    }

    document.addEventListener('pointerdown', handlePointerDown)
    return () => document.removeEventListener('pointerdown', handlePointerDown)
  }, [enabled, ref])
}
