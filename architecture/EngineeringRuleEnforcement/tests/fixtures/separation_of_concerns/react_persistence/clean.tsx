// Fixture: clean React widget - all server traffic through ClientRouter.

import { useEffect } from "react"

export default function CleanWidget() {
  useEffect(() => {
    window.parent.postMessage({
      type: "router:makeCall",
      op: "session_data",
      payload: {},
      call_id: "sd-1",
    }, "*")
  }, [])
  return null
}
