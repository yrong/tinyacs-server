package vertx2.cwmp;

/**
 * Project:  SXA CC
 *
 * The "notification" attribute is an integer in the range from 0-6 that indicates whether (and how) the CPE will
 * notify the ACS when the specified Parameter(s) change in value. The following values are defined by TR-069:
 * Value    Description
 * 0        Notification off.
 *          The CPE need not inform the ACS of a change to the specified Parameter(s).
 * 1        Passive notification.
 *          Whenever the specified Parameter value changes, the CPE MUST include the new value in the ParameterList
 *          in the Inform message that is sent the next time a Session is established to the ACS.
 * 2        Active notification.
 *          Whenever the specified Parameter value changes, the CPE MUST initiate a Session to the ACS, and include the
 *          new value in the ParameterList in the associated Inform message.
 * 3        Passive lightweight notification.
 *          Whenever the specified Parameter value changes, the CPE MUST include the new value in the ParameterList in
 *          the next Lightweight Notification message that is sent.
 * 4        Passive notification with passive lightweight notification.
 *          This combines the requirements of the values 1 (Passive notification) and 3 (Passive lightweight
 *          notification). The two mechanisms operate independently.
 * 5        Active lightweight notification.
 *          Whenever the specified Parameter value changes, the CPE MUST include the new value in the ParameterList in
 *          the associated Lightweight Notification message and send that message.
 * 6        Passive notification with active lightweight notification.
 *          This combines the requirements of the values 1 (Passive notification) and 5 (Active lightweight
 *          notification). The two mechanisms operate independently.
 *
 * @author: ronyang
 */
public class CwmpNotificationValues {
    public static final int NOTIFICATION_UNKNOWN = -1;
    public static final int NOTIFICATION_OFF = 0;
    public static final int NOTIFICATION_PASSIVE = 1;
    public static final int NOTIFICATION_ACTIVE = 2;
    public static final int NOTIFICATION_PASSIVE_LIGHTWEIGHT = 3;
    public static final int NOTIFICATION_ACTIVE_LIGHTWEIGHT = 4;
    public static final int NOTIFICATION_PASSIVE_ACTIVE_LIGHTWEIGHT = 5;
}
