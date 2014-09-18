using System;
using System.Collections.Generic;
using System.Text;

namespace Bend {
    public interface ILogReceiver {
        void handleCommand(LogCommands cmd, byte[] cmddata);

        void requestLogExtension();
        void logStatusChange(long usedLogBytes, long freeLogBytes);
    }
}
