using System;

namespace ActorDb
{
    /// <summary>
    /// Our own logging interface to drop dependency from any specific implementation
    /// </summary>
    public interface IActorDbLogger
    {
        /// <summary>
        /// Process event with importance DEBUG
        /// </summary>
        /// <param name="message"></param>
        /// <param name="args"></param>
        void LogDebug(string  message, params object[] args);

        /// <summary>
        /// Process event with importance ERROR
        /// </summary>
        /// <param name="message"></param>
        /// <param name="args"></param>
        void LogError(string message, params object[] args);

        /// <summary>
        /// Process event with importance ERROR and explicit exception object
        /// </summary>
        /// <param name="exception"></param>
        /// <param name="message"></param>
        /// <param name="args"></param>
        void LogError(Exception exception, string message, params object[] args);
    }
}
