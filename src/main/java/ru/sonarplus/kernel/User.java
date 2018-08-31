package ru.sonarplus.kernel;

/**
 * 
 * @author gubanov
 * 
 */
public class User {
	private final String name;
	private final long id;

	public User(String userName, long userId) {
		this.name = userName;
		this.id = userId;
	}

	public final String getName() {
		return name;
	}

	public final long getId() {
		return id;
	}
}
